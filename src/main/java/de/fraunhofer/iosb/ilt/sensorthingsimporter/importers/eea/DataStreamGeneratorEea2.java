/*
 * Copyright (C) 2020 Fraunhofer IOSB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.ConfigurationException;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.csv.DatastreamGenerator;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.TAG_AREA_TYPE;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.TAG_BEGIN_TIME;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.TAG_COUNTRY_CODE;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.TAG_END_TIME;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.TAG_LOCAL_ID;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.TAG_MEDIA_MONITORED;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.TAG_METADATA;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.TAG_NAMESPACE;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.TAG_NETWORK;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.TAG_NETWORK_NAME;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.TAG_OWNER;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.VALUE_MEDIUM_AIR;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.VALUE_OWNER_EEA;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.EntityCache;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ErrorLog;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.FrostUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UrlUtils;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.Utils;
import de.fraunhofer.iosb.ilt.sta.jackson.ObjectMapperFactory;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Location;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import de.fraunhofer.iosb.ilt.sta.model.Sensor;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.sta.query.Query;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.ParseException;
import org.geojson.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hylke
 */
public class DataStreamGeneratorEea2 implements DatastreamGenerator, AnnotatedConfigurable<SensorThingsService, Object> {

	private static final Logger LOGGER = LoggerFactory.getLogger(DataStreamGeneratorEea2.class.getName());

	private static final String OLD_DS_SEARCH_TEMPLATE = "endswith(Thing/properties/localId,\u0027{STATIONCODE}\u0027) and ObservedProperty/name eq \u0027{PROPERTY}\u0027";

	private static final String TEMPLATE = """
										   {
										   \t"Page": 0,
										   \t"SortBy": null,
										   \t"SortAscending": true,
										   \t"RequestFilter": {
										   \t\t"AssessmentMethodId": {
										   \t\t\t"FieldName": "AssessmentMethodId",
										   \t\t\t"Values": ["$SAMPLING_POINT_ID"]
										   \t\t}
										   \t}
										   }""";

	@ConfigurableField(editor = EditorString.class,
			label = "Stations Url", description = "The url to download the stations CSV file from.")
	@EditorString.EdOptsString(dflt = "https://discomap.eea.europa.eu/App/AQViewer/data?fqn=Airquality_Dissem.b2g.measurements")
	private String stationsUrl;

	@ConfigurableField(editor = EditorString.class,
			label = "Query Template", description = "The POST template to use.")
	@EditorString.EdOptsString(lines = 10,
			dflt = TEMPLATE)
	private String template;

	private FrostUtils frostUtils;

	/**
	 * StationLocalid/SamplingPointLocalId
	 */
	private static final Map<String, EeaStationRecord> SAMPLING_POINTS = new HashMap<>();
	private final EntityCache<String, ObservedProperty> observedPropertyCache = EeaObservedProperty.createObservedPropertyCache();

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx, ConfigEditor<?> configEditor) throws ConfigurationException {
		frostUtils = new FrostUtils(context);
		AnnotatedConfigurable.super.configure(config, context, edtCtx, configEditor);
	}

	private List<Datastream> getDatastreamFor(String filterTemplate, CSVRecord record) throws ServiceFailureException, ImportException {
		String filter = Translator.fillTemplate(filterTemplate, record, Translator.StringType.URL, true);
		SensorThingsService service = frostUtils.getService();
		Query<Datastream> query = service.datastreams().query().orderBy("id asc").filter(filter);
		return query.list().toList();
	}

	@Override
	public Datastream createDatastreamFor(CSVRecord record, ErrorLog errorLog) throws ImportException {
		EeaStationRecord stationRecord = findStation(record);
		if (stationRecord == null) {
			return null;
		}

		Datastream ds = findAndFixOldData(record, stationRecord);
		if (ds != null) {
			return ds;
		}

		return importEntities(stationRecord, record);
	}

	private Datastream findAndFixOldData(CSVRecord record, EeaStationRecord stationRecord) throws ImportException {
		try {
			String stationLocalId = getFromRecord(record, "STATIONCODE");
			String obsProp = getFromRecord(record, "PROPERTY");
			String pointLocalId = getFromRecord(record, "SAMPLINGPOINT_LOCALID");
			List<Datastream> oldDsList = getDatastreamFor(OLD_DS_SEARCH_TEMPLATE, record);
			if (oldDsList.isEmpty()) {
				return null;
			}
			if (oldDsList.size() == 1) {
				Datastream ds = oldDsList.get(0);
				Thing thing = ds.getThing();
				LOGGER.info("Updating localId of Things({}) from {} to {} and name to {}", thing.getId().getUrl(), thing.getProperties().get("localId"), stationLocalId, stationRecord.airQualityStationName);
				thing.getProperties().put(TAG_LOCAL_ID, stationLocalId);
				thing.setName(stationRecord.airQualityStationName);
				frostUtils.update(thing);
				ds.setName(obsProp + " at " + stationRecord.airQualityStationName);
				updateDsLocalId(ds, pointLocalId);
				frostUtils.update(ds);
				return ds;
			}
			if (oldDsList.size() > 1) {
				Datastream mainDs = oldDsList.get(0);
				LOGGER.info("Merging {} datastreams for {} into Datastreams({})", oldDsList.size(), pointLocalId, mainDs.getId().getUrl());
				Thing thing = mainDs.getThing();
				LOGGER.info("Updating localId of Things({}) from {} to {} and name to {}", thing.getId().getUrl(), thing.getProperties().get("localId"), stationLocalId, stationRecord.airQualityStationName);
				thing.getProperties().put(TAG_LOCAL_ID, stationLocalId);
				thing.setName(stationRecord.airQualityStationName);
				frostUtils.update(thing);
				mainDs.setName(obsProp + " at " + stationRecord.airQualityStationName);
				updateDsLocalId(mainDs, pointLocalId);
				frostUtils.update(mainDs);
				Datastream mainDsOnlyId = mainDs.withOnlyId();
				for (int idx = 1; idx < oldDsList.size(); idx++) {
					int obsCount = 0;
					Datastream badDs = oldDsList.get(idx);
					Iterator<Observation> it = badDs.observations().query().orderBy("phenomenonTime asc").list().fullIterator();
					while (it.hasNext()) {
						Observation badObs = it.next();
						badObs.setDatastream(mainDsOnlyId);
						frostUtils.update(badObs);
						obsCount++;
					}
					LOGGER.info("Moved {} Observations from Datastreams({})", obsCount, badDs.getId().getUrl());
					frostUtils.delete(Arrays.asList(badDs), 1);
				}
				return mainDs;
			}
		} catch (ServiceFailureException ex) {
			LOGGER.error("Failed to update old data: {}", ex.getMessage());
		}
		return null;
	}

	private void updateDsLocalId(Datastream ds, String localId) {
		Map<String, Object> properties = ds.getProperties();
		Object localIds = properties.get(TAG_LOCAL_ID);
		if (localIds instanceof String lids) {
			List<String> localIdList = new ArrayList<>();
			localIdList.add(lids);
			if (!localId.equals(lids)) {
				localIdList.add(localId);
			}
			properties.put(TAG_LOCAL_ID, localIdList);
			return;
		}
		if (localIds instanceof List localIdList) {
			if (!localIdList.contains(localId)) {
				localIdList.add(localId);
			}
			return;
		}
		LOGGER.error("Unknown type of LocalId property of Datastreams({})! {}", ds.getId().getUrl(), localIds);
	}

	private Datastream importEntities(EeaStationRecord sr, CSVRecord record) throws ImportException {
		String pointLocalId = getFromRecord(record, "SAMPLINGPOINT_LOCALID");
		String obsPropName = getFromRecord(record, "PROPERTY");
		ObservedProperty observedProperty = observedPropertyCache.getByName(obsPropName);
		if (observedProperty == null) {
			LOGGER.error("Found no ObservedProperty for {}", obsPropName);
			return null;
		}

		Point point = new Point(
				sr.longitude.setScale(6, RoundingMode.HALF_EVEN).doubleValue(),
				sr.latitude.setScale(6, RoundingMode.HALF_EVEN).doubleValue());

		Map<String, Object> stationProps = new HashMap<>();
		stationProps.put(TAG_LOCAL_ID, sr.airQualityStationEoICode);
		stationProps.put(TAG_COUNTRY_CODE, sr.countrycode);
		stationProps.put(TAG_NETWORK, sr.airQualityNetwork);
		stationProps.put(TAG_NETWORK_NAME, sr.airQualityNetworkName);
		stationProps.put(TAG_OWNER, VALUE_OWNER_EEA);
		stationProps.put(TAG_NAMESPACE, sr.namespace);
		stationProps.put(TAG_MEDIA_MONITORED, VALUE_MEDIUM_AIR);
		stationProps.put(TAG_AREA_TYPE, sr.airQualityStationArea);
		stationProps.put(TAG_BEGIN_TIME, sr.observationDateBegin);
		if (!sr.observationDateEnd.isEmpty()) {
			stationProps.put(TAG_END_TIME, sr.observationDateEnd);
		}
		stationProps.put(TAG_METADATA, stationsUrl);

		Map<String, Object> locationProps = new HashMap<>();
		locationProps.put(TAG_LOCAL_ID, sr.airQualityStationEoICode);
		locationProps.put(TAG_COUNTRY_CODE, sr.countrycode);
		locationProps.put(TAG_OWNER, VALUE_OWNER_EEA);
		locationProps.put(TAG_NAMESPACE, sr.namespace);
		locationProps.put(TAG_METADATA, stationsUrl);
		locationProps.put(TAG_AREA_TYPE, sr.airQualityStationArea);

		Map<String, Object> sensorProps = new HashMap<>();
		sensorProps.put(TAG_LOCAL_ID, sr.assessmentMethodId);
		sensorProps.put(EeaConstants.TAG_SAMPLING_METHOD, sr.samplingMethod);
		sensorProps.put(TAG_COUNTRY_CODE, sr.countrycode);
		sensorProps.put(TAG_OWNER, VALUE_OWNER_EEA);
		sensorProps.put(TAG_NAMESPACE, sr.namespace);
		sensorProps.put(TAG_METADATA, stationsUrl);

		Map<String, Object> dsProps = new HashMap<>();
		dsProps.put(TAG_LOCAL_ID, new String[]{pointLocalId});
		dsProps.put(TAG_COUNTRY_CODE, sr.countrycode);
		dsProps.put(TAG_OWNER, VALUE_OWNER_EEA);
		dsProps.put(TAG_NAMESPACE, sr.namespace);
		dsProps.put(TAG_METADATA, stationsUrl);

		try {
			String filter = "properties/" + TAG_LOCAL_ID + " eq " + Utils.quoteForUrl(sr.airQualityStationEoICode);
			Location location = frostUtils.findOrCreateLocation(
					filter,
					sr.airQualityStationName,
					"Location of station " + sr.airQualityStationEoICode,
					locationProps,
					point,
					null);
			Thing thing = frostUtils.findOrCreateThing(
					filter,
					sr.airQualityStationName,
					"Measurement station " + sr.airQualityStationName,
					stationProps,
					location,
					null);

			filter = "properties/" + TAG_LOCAL_ID + " eq " + Utils.quoteForUrl(sr.assessmentMethodId);
			Sensor sensor = frostUtils.findOrCreateSensor(
					filter,
					sr.processId,
					"Sensor " + sr.processId,
					"text/html",
					sr.measurementEquipment,
					sensorProps,
					null);

			String valueUnit = getFromRecord(record, "UNIT", "value_unit", "UnitOfMeasurement");
			if (valueUnit == null) {
				throw new ImportException("Could not find unit in record.");
			}
			UnitOfMeasurement uom = new UnitOfMeasurement(valueUnit, valueUnit, valueUnit);
			filter = "properties/" + TAG_LOCAL_ID + " eq " + Utils.quoteForUrl(pointLocalId) + " or " + Utils.quoteForUrl(pointLocalId) + " in properties/" + TAG_LOCAL_ID;
			Datastream ds = frostUtils.findOrCreateDatastream(
					filter,
					observedProperty.getName() + " at " + thing.getName(),
					observedProperty.getName() + " at " + thing.getName(),
					dsProps,
					uom, thing, observedProperty, sensor, null);
			return ds;
		} catch (ServiceFailureException ex) {
			LOGGER.debug("Exception: {}", ex.getMessage());
			throw new ImportException(ex);
		}
	}

	private String getFromRecord(CSVRecord record, String... names) {
		for (String name : names) {
			try {
				return record.get(name).trim();
			} catch (IllegalArgumentException ex) {
				// It's fine
			}
		}
		return null;
	}

	private EeaStationRecord findStation(CSVRecord record) throws ImportException {
		loadObservedProperties();
		String pointLocalId = getFromRecord(record, "SAMPLINGPOINT_LOCALID", "samplingpoint_localid", "SamplingPoint");
		if (Utils.isNullOrEmpty(pointLocalId)) {
			return null;
		}
		EeaStationRecord station = SAMPLING_POINTS.get(pointLocalId);
		if (station != null) {
			return station;
		}
		String postData = StringUtils.replace(template, "$SAMPLING_POINT_ID", pointLocalId);

		JsonNode tree;
		try {
			String data = UrlUtils.postToUrl(stationsUrl, postData, null, null).data;
			tree = ObjectMapperFactory.get().readTree(data);
			JsonNode rows = tree.get("Rows");
			if (rows == null || !rows.isArray() || rows.size() == 0) {
				LOGGER.warn("No data received for sampling point {}", pointLocalId);
				return null;
			}
			if (rows.size() > 1) {
				LOGGER.warn("Multiple data received for sampling point {}", pointLocalId);
			}
			JsonNode samplingPoint = rows.get(0);
			station = new EeaStationRecord(samplingPoint);
			SAMPLING_POINTS.put(pointLocalId, station);
			return station;
		} catch (IOException | ParseException ex) {
			LOGGER.debug("IOException parsing JSON response: {}", ex.getMessage());
			throw new ImportException("Failed to parse station JSON data", ex);
		}
	}

	private void loadObservedProperties() throws ImportException {
		if (!observedPropertyCache.isEmpty()) {
			return;
		}
		try {
			observedPropertyCache.load(
					frostUtils.getService().observedProperties(),
					"",
					"id,name,description,definition,properties",
					"");
			EeaObservedProperty.importObservedProperties(frostUtils, observedPropertyCache);
		} catch (ServiceFailureException ex) {
			LOGGER.debug("Exception: {}", ex.getMessage());
			throw new ImportException("Failed to load observed properties", ex);
		}

	}

	private static class EeaStationRecord {

		String countrycode;
		String country;
		String timezone;
		String namespace;
		String airQualityNetwork;
		String airQualityNetworkName;
		String airQualityStationEoICode;
		String airQualityStationNatCode;
		String processId;
		String assessmentMethodId;
		String sample;
		String samplingMethod;
		String observationDateBegin;
		String observationDateEnd;

		BigDecimal longitude;
		BigDecimal latitude;
		BigDecimal altitude;

		String measurementType;
		String airQualityStationType;
		String airQualityStationArea;
		String airQualityStationName;
		String equivalenceDemonstrated;
		String measurementEquipment;
		BigDecimal inletHeight;
		String inletHeightUnit;
		BigDecimal buildingDistance;
		String buildingDistanceUnit;
		BigDecimal kerbDistance;
		String kerbDistanceUnit;

		public EeaStationRecord(JsonNode samplingPoint) {
			countrycode = deQuote(getString(samplingPoint, "AirQualityStationEoICode")).substring(0, 2);
			country = deQuote(getString(samplingPoint, "Country"));
			timezone = deQuote(getString(samplingPoint, "Timezone"));
			namespace = deQuote(getString(samplingPoint, "B2G_Namespace"));
			airQualityNetwork = deQuote(getString(samplingPoint, "AirQualityNetwork"));
			airQualityNetworkName = deQuote(getString(samplingPoint, "AirQualityNetworkName"));
			airQualityStationEoICode = deQuote(getString(samplingPoint, "AirQualityStationEoICode"));
			airQualityStationNatCode = deQuote(getString(samplingPoint, "AirQualityStationNatCode"));

			processId = deQuote(getString(samplingPoint, "ProcessId"));
			assessmentMethodId = deQuote(getString(samplingPoint, "AssessmentMethodId"));

			observationDateBegin = deQuote(getString(samplingPoint, "OperationalActivityBegin"));
			observationDateEnd = deQuote(getString(samplingPoint, "OperationalActivityEnd"));

			longitude = getNumber(samplingPoint, "Longitude");
			latitude = getNumber(samplingPoint, "Latitude");
			altitude = getNumber(samplingPoint, "Altitude");

			measurementType = getString(samplingPoint, "MeasurementType");
			airQualityStationType = getString(samplingPoint, "AirQualityStationType");
			airQualityStationArea = getString(samplingPoint, "AirQualityStationArea");
			airQualityStationName = deQuote(getString(samplingPoint, "AQStationName"));

			equivalenceDemonstrated = getString(samplingPoint, "EquivalenceDemonstrated");
			measurementEquipment = getString(samplingPoint, "MeasurementEquipment");

			inletHeight = getNumber(samplingPoint, "InletHeight");
			inletHeightUnit = getString(samplingPoint, "InletHeightUnit");
			buildingDistance = getNumber(samplingPoint, "BuildingDistance");
			buildingDistanceUnit = getString(samplingPoint, "BuilldingDistanceUnit");
			kerbDistance = getNumber(samplingPoint, "KerbDistance");
			kerbDistanceUnit = getString(samplingPoint, "KerbDistanceUnit");

			samplingMethod = getString(samplingPoint, "SamplingMethod");

		}

		private BigDecimal getNumber(JsonNode node, String property) {
			JsonNode value = node.get(property);
			return value.decimalValue();
		}

		private String getString(JsonNode node, String property) {
			JsonNode value = node.get(property);
			if (value == null) {
				return null;
			}
			if (value.isTextual()) {
				return value.textValue();
			}
			LOGGER.error("No text value for {}: {}", property, value);
			return "";
		}

		private String deQuote(String input) {
			if (Utils.isNullOrEmpty(input)) {
				return input;
			}
			return StringUtils.remove(input, '"');
		}
	}
}
