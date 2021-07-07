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
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.TAG_OWNER;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.VALUE_MEDIUM_AIR;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.VALUE_OWNER_EEA;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.EntityCache;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.FrostUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UrlUtils;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.Utils;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Location;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import de.fraunhofer.iosb.ilt.sta.model.Sensor;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.geojson.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hylke
 */
public class DataStreamGeneratorEea implements DatastreamGenerator, AnnotatedConfigurable<SensorThingsService, Object> {

	private static final Logger LOGGER = LoggerFactory.getLogger(DataStreamGeneratorEea.class.getName());

	@ConfigurableField(editor = EditorString.class,
			label = "Stations Url", description = "The url to download the stations CSV file from.")
	@EditorString.EdOptsString(dflt = "http://discomap.eea.europa.eu/map/fme/metadata/PanEuropean_metadata.csv")
	private String stationsUrl;

	private FrostUtils frostUtils;

	/**
	 * StationLocalid/SamplingPointLocalId
	 */
	private static final Map<String, Map<String, Map<String, EeaStationRecord>>> STATIONS = new HashMap<>();
	private final EntityCache<String, ObservedProperty> observedPropertyCache = EeaObservedProperty.createObservedPropertyCache();

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx, ConfigEditor<?> configEditor) throws ConfigurationException {
		frostUtils = new FrostUtils(context);
		AnnotatedConfigurable.super.configure(config, context, edtCtx, configEditor);
	}

	@Override
	public Datastream createDatastreamFor(CSVRecord record) throws ImportException {
		EeaStationRecord stationRecord = findStation(record);
		if (stationRecord == null) {
			return null;
		}
		return importEntities(stationRecord, record);
	}

	private Datastream importEntities(EeaStationRecord sr, CSVRecord record) throws ImportException {
		String name = sr.airQualityStation;
		Point point = new Point(
				new BigDecimal(sr.longitude).setScale(6, RoundingMode.HALF_EVEN).doubleValue(),
				new BigDecimal(sr.latitude).setScale(6, RoundingMode.HALF_EVEN).doubleValue());

		Map<String, Object> stationProps = new HashMap<>();
		stationProps.put(TAG_LOCAL_ID, sr.airQualityStation);
		stationProps.put(TAG_COUNTRY_CODE, sr.countrycode);
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
		locationProps.put(TAG_LOCAL_ID, sr.airQualityStation);
		locationProps.put(TAG_COUNTRY_CODE, sr.countrycode);
		locationProps.put(TAG_OWNER, VALUE_OWNER_EEA);
		locationProps.put(TAG_NAMESPACE, sr.namespace);
		locationProps.put(TAG_METADATA, stationsUrl);
		locationProps.put(TAG_AREA_TYPE, sr.airQualityStationArea);

		Map<String, Object> sensorProps = new HashMap<>();
		sensorProps.put(TAG_LOCAL_ID, sr.samplingProces);
		sensorProps.put(TAG_COUNTRY_CODE, sr.countrycode);
		sensorProps.put(TAG_OWNER, VALUE_OWNER_EEA);
		sensorProps.put(TAG_NAMESPACE, sr.namespace);
		sensorProps.put(TAG_METADATA, stationsUrl);

		Map<String, Object> dsProps = new HashMap<>();
		dsProps.put(TAG_LOCAL_ID, sr.samplingPoint);
		dsProps.put(TAG_COUNTRY_CODE, sr.countrycode);
		dsProps.put(TAG_OWNER, VALUE_OWNER_EEA);
		dsProps.put(TAG_NAMESPACE, sr.namespace);
		dsProps.put(TAG_METADATA, stationsUrl);

		if (Utils.isNullOrEmpty(sr.samplingProces)) {
			LOGGER.error("Station with empty samplingProcess.");
			return null;
		}
		try {
			String filter = "properties/" + TAG_LOCAL_ID + " eq " + Utils.quoteForUrl(sr.airQualityStation);
			Location location = frostUtils.findOrCreateLocation(
					filter,
					name,
					"Location of station " + name,
					locationProps,
					point,
					null);
			Thing thing = frostUtils.findOrCreateThing(
					filter,
					name,
					"Measurement station " + name,
					stationProps,
					location,
					null);

			filter = "properties/" + TAG_LOCAL_ID + " eq " + Utils.quoteForUrl(sr.samplingProces);
			Sensor sensor = frostUtils.findOrCreateSensor(
					filter,
					sr.samplingProces,
					"Sensor " + sr.samplingProces,
					"text/html",
					sr.measurementEquipment,
					sensorProps,
					null);
			ObservedProperty observedProperty = observedPropertyCache.get(FrostUtils.afterLastSlash(sr.airPollutantCode).trim());

			String valueUnit = getFromRecord(record, "value_unit", "UnitOfMeasurement");
			if (valueUnit == null) {
				throw new ImportException("Could not find unit in record.");
			}
			UnitOfMeasurement uom = new UnitOfMeasurement(valueUnit, valueUnit, valueUnit);
			filter = "properties/" + TAG_LOCAL_ID + " eq " + Utils.quoteForUrl(sr.samplingPoint);
			Datastream ds = frostUtils.findOrCreateDatastream(
					filter,
					sr.samplingPoint,
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
				return record.get(name);
			} catch (IllegalArgumentException ex) {
				// It's fine
			}
		}
		return null;
	}

	private EeaStationRecord findStation(CSVRecord record) throws ImportException {
		loadStationData();
		loadObservedProperties();
		String stationLocalId = getFromRecord(record, "station_localid", "AirQualityStation");
		String pointLocalId = getFromRecord(record, "samplingpoint_localid", "SamplingPoint");
		String processLocalId = getFromRecord(record, "samplingprocess_localid", "SamplingProcess");
		return STATIONS
				.getOrDefault(stationLocalId, Collections.emptyMap())
				.getOrDefault(pointLocalId, Collections.emptyMap())
				.get(processLocalId);
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

	private void loadStationData() throws ImportException {
		if (!STATIONS.isEmpty()) {
			return;
		}
		LOGGER.info("Loading station MetaData from {}", stationsUrl);
		String data;
		try {
			data = UrlUtils.fetchFromUrl(stationsUrl);
		} catch (IOException ex) {
			LOGGER.error("Failed to handle URL: {}; {}", stationsUrl, ex.getMessage());
			throw new ImportException(ex);
		}
		try {
			CSVParser stationParser = CSVParser.parse(
					data,
					CSVFormat.DEFAULT
							.withDelimiter('\t')
							.withFirstRecordAsHeader());
			Iterator<CSVRecord> iterator = stationParser.iterator();
			while (iterator.hasNext()) {
				CSVRecord record = iterator.next();
				EeaStationRecord station = new EeaStationRecord(record);
				STATIONS.computeIfAbsent(station.airQualityStation, (t) -> new HashMap<>())
						.computeIfAbsent(station.samplingPoint, (t) -> new HashMap<>())
						.put(station.samplingProces, station);
			}

		} catch (IOException ex) {
			LOGGER.debug("IOException parsing CSV file: {}", ex.getMessage());
			throw new ImportException("Failed to parse station CSV file", ex);
		}
	}

	private static class EeaStationRecord {

		String countrycode;
		String timezone;
		String namespace;
		String airQualityNetwork;
		String airQualityStation;
		String airQualityStationEoICode;
		String airQualityStationNatCode;
		String samplingPoint;
		String samplingProces;
		String sample;
		String airPollutantCode;
		String observationDateBegin;
		String observationDateEnd;
		String projection;
		String longitude;
		String latitude;
		String altitude;
		String measurementType;
		String airQualityStationType;
		String airQualityStationArea;
		String equivalenceDemonstrated;
		String measurementEquipment;
		String inletHeight;
		String buildingDistance;
		String kerbDistance;

		public EeaStationRecord(CSVRecord record) {
			countrycode = record.get("Countrycode");
			timezone = record.get("Timezone");
			namespace = record.get("Namespace");
			airQualityNetwork = record.get("AirQualityNetwork");
			airQualityStation = record.get("AirQualityStation");
			airQualityStationEoICode = record.get("AirQualityStationEoICode");
			airQualityStationNatCode = record.get("AirQualityStationNatCode");
			samplingPoint = record.get("SamplingPoint");
			samplingProces = record.get("SamplingProces");
			sample = record.get("Sample");
			airPollutantCode = record.get("AirPollutantCode");
			observationDateBegin = record.get("ObservationDateBegin");
			observationDateEnd = record.get("ObservationDateEnd");
			projection = record.get("Projection");
			longitude = record.get("Longitude");
			latitude = record.get("Latitude");
			altitude = record.get("Altitude");
			measurementType = record.get("MeasurementType");
			airQualityStationType = record.get("AirQualityStationType");
			airQualityStationArea = record.get("AirQualityStationArea");
			equivalenceDemonstrated = record.get("EquivalenceDemonstrated");
			measurementEquipment = record.get("MeasurementEquipment");
			inletHeight = record.get("InletHeight");
			buildingDistance = record.get("BuildingDistance");
			kerbDistance = record.get("KerbDistance");

			if (Utils.isNullOrEmpty(samplingProces)) {
				samplingProces = "Unknown";
			}
		}
	}
}
