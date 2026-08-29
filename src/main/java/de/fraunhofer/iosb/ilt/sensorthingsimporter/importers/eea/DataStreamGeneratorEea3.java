/*
 * Copyright (C) 2026 Fraunhofer Institut IOSB, Fraunhoferstr. 1, D 76131
 * Karlsruhe, Germany.
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

import static de.fraunhofer.iosb.ilt.frostclient.models.CommonProperties.EP_NAME;
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

import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.frostclient.SensorThingsService;
import de.fraunhofer.iosb.ilt.frostclient.exception.ServiceFailureException;
import de.fraunhofer.iosb.ilt.frostclient.json.SimpleJsonMapper;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.frostclient.model.ModelRegistry;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11MultiDatastream;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.frostclient.utils.StringHelper;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.csv.DatastreamGenerator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.records.Tuple;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.CsvUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.EntityCache;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ErrorLog;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.FrostUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator.CsvTuple;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UrlUtils;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.ParseException;
import org.geojson.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;

/**
 * Generates datastreams from an EEA record and EEA metadata.
 */
public class DataStreamGeneratorEea3 implements DatastreamGenerator, de.fraunhofer.iosb.ilt.sensorthingsimporter.records.DatastreamGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataStreamGeneratorEea3.class.getName());

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

    @ConfigurableField(editor = EditorString.class,
            label = "Samplingpoint Template", description = "The template to use to extract the sampling point from the record.")
    @EditorString.EdOptsString(lines = 1,
            dflt = "{Samplingpoint[3:]}")
    private String samplingPointTemplate;

    private FrostUtils frostUtils;
    private SensorThingsV11Sensing mdl11;
    private SensorThingsV11MultiDatastream mdlMds;

    /**
     * StationLocalid/SamplingPointLocalId
     */
    private static final Map<String, EeaStationRecord> SAMPLING_POINTS = new HashMap<>();
    private final EntityCache<String> observedPropertyCache = EeaObservedProperty.createObservedPropertyCache();

    @Override
    public void init(SensorThingsService service) throws ImportException {
        frostUtils = new FrostUtils(service);
        final ModelRegistry mr = service.getModelRegistry();
        mdl11 = mr.getModel(SensorThingsV11Sensing.class);
        mdlMds = mr.getModel(SensorThingsV11MultiDatastream.class);
    }

    private List<Entity> getDatastreamFor(String filterTemplate, Tuple record) throws ServiceFailureException, ImportException {
        String filter = Translator.fillTemplate(filterTemplate, record, Translator.StringType.URL, true);
        SensorThingsService service = frostUtils.getService();
        return service.query(mdl11.etDatastream)
                .orderBy("id asc")
                .filter(filter)
                .list()
                .toList();
    }

    @Override
    public Entity createDatastreamFor(Tuple record, ErrorLog errorLog) throws ImportException {
        EeaStationRecord stationRecord = findStation(record);
        if (stationRecord == null) {
            return null;
        }

        Entity ds = findAndFixOldData(record, stationRecord);
        if (ds != null) {
            return ds;
        }

        return importEntities(stationRecord, record);
    }

    @Override
    public Entity createDatastreamFor(CSVRecord record, ErrorLog errorLog) throws ImportException {
        return createDatastreamFor(CsvTuple.of(record, true), errorLog);
    }

    private Entity findAndFixOldData(Tuple record, EeaStationRecord stationRecord) throws ImportException {
        return null;
    }

    private Entity importEntities(EeaStationRecord sr, Tuple record) throws ImportException {
        String pointLocalId = Translator.fillTemplate(samplingPointTemplate, record);
        String obsPropLocalId = getFromRecord(record, "Pollutant");
        Entity observedProperty = observedPropertyCache.get(obsPropLocalId);
        if (observedProperty == null) {
            LOGGER.error("Found no ObservedProperty for {}", obsPropLocalId);
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
            String filter = "properties/" + TAG_LOCAL_ID + " eq " + StringHelper.quoteForUrl(sr.airQualityStationEoICode);
            Entity location = frostUtils.findOrCreateLocation(
                    filter,
                    sr.airQualityStationName,
                    "Location of station " + sr.airQualityStationEoICode,
                    locationProps,
                    point,
                    null);
            Entity thing = frostUtils.findOrCreateThing(
                    filter,
                    sr.airQualityStationName,
                    "Measurement station " + sr.airQualityStationName,
                    stationProps,
                    location,
                    null);

            filter = "properties/" + TAG_LOCAL_ID + " eq " + StringHelper.quoteForUrl(sr.assessmentMethodId);
            Entity sensor = frostUtils.findOrCreateSensor(
                    filter,
                    sr.processId,
                    "Sensor " + sr.processId,
                    "text/html",
                    sr.measurementEquipment,
                    sensorProps,
                    null);

            String unit = CsvUtils.findMatch("properties/recommendedUnit", null, observedProperty);
            if (unit == null) {
                unit = getFromRecord(record, "Unit");
            }
            if (unit == null) {
                throw new ImportException("Could not find unit in record.");
            }
            UnitOfMeasurement uom = new UnitOfMeasurement(unit, unit, unit);
            filter = "properties/" + TAG_LOCAL_ID + " eq " + StringHelper.quoteForUrl(pointLocalId) + " or " + StringHelper.quoteForUrl(pointLocalId) + " in properties/" + TAG_LOCAL_ID;
            Entity ds = frostUtils.findOrCreateDatastream(
                    filter,
                    observedProperty.getProperty(EP_NAME) + " at " + thing.getProperty(EP_NAME),
                    observedProperty.getProperty(EP_NAME) + " at " + thing.getProperty(EP_NAME),
                    dsProps,
                    uom, thing, observedProperty, sensor, null);
            return ds;
        } catch (ServiceFailureException ex) {
            LOGGER.debug("Exception: {}", ex.getMessage());
            throw new ImportException(ex);
        }
    }

    private String getFromRecord(Tuple record, String... names) {
        for (String name : names) {
            if (!record.isMapped(name)) {
                continue;
            }
            try {
                return record.getString(name).trim();
            } catch (IllegalArgumentException ex) {
                // It's fine
            }
        }
        return null;
    }

    private EeaStationRecord findStation(Tuple record) throws ImportException {
        loadObservedProperties();
        String pointLocalId = Translator.fillTemplate(samplingPointTemplate, record);
        //getFromRecord(record, "SAMPLINGPOINT_LOCALID", "samplingpoint_localid", "SamplingPoint", "Samplingpoint");
        if (StringHelper.isNullOrEmpty(pointLocalId)) {
            return null;
        }
        EeaStationRecord station = SAMPLING_POINTS.get(pointLocalId);
        if (station != null) {
            return station;
        }
        String postData = StringUtils.replace(template, "$SAMPLING_POINT_ID", pointLocalId);

        JsonNode tree;
        try {
            String data = UrlUtils.postToUrl(stationsUrl, postData, null, null).getDataString();
            tree = SimpleJsonMapper.getSimpleObjectMapper().readTree(data);
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
                    frostUtils.getService().dao(mdl11.etObservedProperty),
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
            if (value == null) {
                return null;
            }
            if (value.isNumber()) {
                return value.decimalValue();
            }
            LOGGER.debug("No numeric value for {}: {}", property, value);
            return null;
        }

        private String getString(JsonNode node, String property) {
            JsonNode value = node.get(property);
            if (value == null) {
                return null;
            }
            if (value.isString()) {
                return value.stringValue();
            }
            LOGGER.debug("No text value for {}: {}", property, value);
            return "";
        }

        private String deQuote(String input) {
            if (StringHelper.isNullOrEmpty(input)) {
                return input;
            }
            return StringUtils.remove(input, '"');
        }
    }
}
