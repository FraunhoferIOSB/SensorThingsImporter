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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.csv;

import static de.fraunhofer.iosb.ilt.frostclient.models.CommonProperties.EP_DESCRIPTION;
import static de.fraunhofer.iosb.ilt.frostclient.models.CommonProperties.EP_NAME;
import static de.fraunhofer.iosb.ilt.frostclient.models.CommonProperties.EP_PROPERTIES;
import static de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing.EP_OBSERVATIONTYPE;

import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.frostclient.SensorThingsService;
import de.fraunhofer.iosb.ilt.frostclient.exception.ServiceFailureException;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.frostclient.model.EntitySet;
import de.fraunhofer.iosb.ilt.frostclient.model.ModelRegistry;
import de.fraunhofer.iosb.ilt.frostclient.model.property.type.TypeComplex;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11MultiDatastream;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.MapValue;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.frostclient.query.Query;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ErrorLog;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.FrostUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.JsonUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator.StringType;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStreamGeneratorNames implements DatastreamGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataStreamGeneratorNames.class.getName());

    @ConfigurableField(editor = EditorString.class,
            label = "Thing Filter", description = "A filter that will be used to find the Thing for the Datastream to create.")
    @EditorString.EdOptsString(dflt = "name eq {nameColumn}", lines = 3)
    private String filterThing;

    @ConfigurableField(editor = EditorString.class,
            label = "Sensor Filter", description = "A filter that will be used to find the Sensor for the Datastream to create.")
    @EditorString.EdOptsString(dflt = "name eq {nameColumn}", lines = 3)
    private String filterSensor;

    @ConfigurableField(editor = EditorString.class,
            label = "ObsProp Filter", description = "A filter that will be used to find the Sensor for the Datastream to create.")
    @EditorString.EdOptsString(dflt = "name eq {nameColumn}", lines = 3)
    private String filterObsProp;

    @ConfigurableField(editor = EditorString.class,
            label = "Name", description = "The name")
    @EditorString.EdOptsString(dflt = "{nameColumn}", lines = 1)
    private String templateName;

    @ConfigurableField(editor = EditorString.class,
            label = "Description", description = "The description")
    @EditorString.EdOptsString(dflt = "{descColumn}", lines = 1)
    private String templateDescription;

    @ConfigurableField(editor = EditorString.class,
            label = "Properties", description = "Template used to generate properties.")
    @EditorString.EdOptsString(lines = 4)
    private String templateProperties;

    @ConfigurableField(editor = EditorString.class,
            label = "UoM Name", description = "The name of the Unit Of Measurement")
    @EditorString.EdOptsString(dflt = "{column}", lines = 1)
    private String templateUomName;

    @ConfigurableField(editor = EditorString.class,
            label = "UoM Symbol", description = "The symbol of the Unit Of Measurement")
    @EditorString.EdOptsString(dflt = "{column}", lines = 1)
    private String templateUomSymbol;

    @ConfigurableField(editor = EditorString.class,
            label = "UoM Definition", description = "The definition of the Unit Of Measurement")
    @EditorString.EdOptsString(dflt = "{column}", lines = 1)
    private String templateUomDef;

    @ConfigurableField(editor = EditorString.class,
            label = "Obs Type", description = "The type of observations")
    @EditorString.EdOptsString(dflt = "\"http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement", lines = 1)
    private String templateObsType;

    private final Map<String, Entity> cacheThings = new HashMap<>();
    private final Map<String, Entity> cacheSensors = new HashMap<>();
    private final Map<String, Entity> cacheObsProps = new HashMap<>();

    private SensorThingsService service;
    private FrostUtils frostUtils;
    private SensorThingsV11Sensing mdl11;
    private SensorThingsV11MultiDatastream mdlMds;

    @Override
    public void init(SensorThingsService service) throws ImportException {
        this.service = service;
        this.frostUtils = new FrostUtils(service);
        final ModelRegistry mr = service.getModelRegistry();
        mdl11 = mr.getModel(SensorThingsV11Sensing.class);
        mdlMds = mr.getModel(SensorThingsV11MultiDatastream.class);
    }

    @Override
    public Entity createDatastreamFor(CSVRecord record, ErrorLog errorLog) throws ImportException {
        Entity thing = getThingFor(record, errorLog);
        Entity sensor = getSensorFor(record, errorLog);
        Entity obsProp = getObsPropFor(record, errorLog);
        if (thing == null || sensor == null || obsProp == null) {
            return null;
        }
        Entity ds = mdl11.newDatastream();
        ds.setProperty(EP_NAME, Translator.fillTemplate(templateName, record, StringType.PLAIN, true));
        ds.setProperty(EP_DESCRIPTION, Translator.fillTemplate(templateDescription, record, StringType.PLAIN, true));
        ds.setProperty(EP_OBSERVATIONTYPE, Translator.fillTemplate(templateObsType, record, StringType.PLAIN, true));
        String propertiesString = Translator.fillTemplate(templateProperties, record, StringType.JSON, false);
        ds.setProperty(EP_PROPERTIES, new MapValue(TypeComplex.STA_MAP, JsonUtils.jsonToMap(propertiesString)));
        UnitOfMeasurement uom = new UnitOfMeasurement(
                Translator.fillTemplate(templateUomName, record, StringType.PLAIN, true),
                Translator.fillTemplate(templateUomSymbol, record, StringType.PLAIN, true),
                Translator.fillTemplate(templateUomDef, record, StringType.PLAIN, true));
        ds.setProperty(SensorThingsV11Sensing.EP_UNITOFMEASUREMENT, uom);
        ds.setProperty(mdl11.npDatastreamThing, thing);
        ds.setProperty(mdl11.npDatastreamSensor, sensor);
        ds.setProperty(mdl11.npDatastreamObservedproperty, obsProp);
        try {
            frostUtils.create(ds);
        } catch (ServiceFailureException ex) {
            throw new ImportException("Failed to create Datastream", ex);
        }
        return ds;
    }

    public Entity getThingFor(CSVRecord record, ErrorLog errorLog) throws ImportException {
        try {
            String filter = Translator.fillTemplate(filterThing, record, StringType.URL, true);
            Entity t = getThingFor(filter, errorLog);
            return t;
        } catch (ServiceFailureException ex) {
            LOGGER.error("Failed to fetch datastream.", ex);
            throw new IllegalArgumentException(ex);
        }
    }

    public Entity getSensorFor(CSVRecord record, ErrorLog errorLog) throws ImportException {
        try {
            String filter = Translator.fillTemplate(filterSensor, record, StringType.URL, true);
            Entity s = getSensorFor(filter, errorLog);
            return s;
        } catch (ServiceFailureException ex) {
            LOGGER.error("Failed to fetch datastream.", ex);
            throw new IllegalArgumentException(ex);
        }
    }

    public Entity getObsPropFor(CSVRecord record, ErrorLog errorLog) throws ImportException {
        try {
            String filter = Translator.fillTemplate(filterObsProp, record, StringType.URL, true);
            Entity o = getObsPropFor(filter, errorLog);
            return o;
        } catch (ServiceFailureException ex) {
            LOGGER.error("Failed to fetch datastream.", ex);
            throw new IllegalArgumentException(ex);
        }
    }

    private Entity getThingFor(String filter, ErrorLog errorLog) throws ServiceFailureException, ImportException {
        Entity t = cacheThings.get(filter);
        if (t != null) {
            return t;
        }
        if (cacheThings.containsKey(filter)) {
            // We previously had found nothing. Don't search again.
            return null;
        }
        Query query = service.query(mdl11.etThing).filter(filter);
        EntitySet streams = query.list();
        if (streams.size() > 1) {
            LOGGER.error("Found incorrect number of Things: {} for filter: {}", streams.size(), filter);
            throw new ImportException("Found incorrect number of Things: " + streams.size() + " for filter: " + filter);
        } else if (!streams.isEmpty()) {
            t = streams.iterator().next();
            LOGGER.debug("Found Thing {} for filter {}.", t, filter);
        }
        if (t == null) {
            LOGGER.error("Found no Thing for filter: {}.", filter);
            errorLog.addError("Thing not found");
        }
        cacheThings.put(filter, t);
        return t;
    }

    private Entity getSensorFor(String filter, ErrorLog errorLog) throws ServiceFailureException, ImportException {
        Entity s = cacheSensors.get(filter);
        if (s != null) {
            return s;
        }
        if (cacheSensors.containsKey(filter)) {
            // We previously had found nothing. Don't search again.
            return null;
        }
        Query query = service.query(mdl11.etSensor).filter(filter);
        EntitySet streams = query.list();
        if (streams.size() > 1) {
            LOGGER.error("Found incorrect number of Sensors: {} for filter: {}", streams.size(), filter);
            throw new ImportException("Found incorrect number of Sensors: " + streams.size() + " for filter: " + filter);
        } else if (!streams.isEmpty()) {
            s = streams.iterator().next();
            LOGGER.debug("Found Sensor {} for filter {}.", s, filter);
        }
        if (s == null) {
            LOGGER.error("Found no Sensor for filter: {}.", filter);
            errorLog.addError("Sensor not found");
        }
        cacheSensors.put(filter, s);
        return s;
    }

    private Entity getObsPropFor(String filter, ErrorLog errorLog) throws ServiceFailureException, ImportException {
        Entity o = cacheObsProps.get(filter);
        if (o != null) {
            return o;
        }
        if (cacheObsProps.containsKey(filter)) {
            // We previously had found nothing. Don't search again.
            return null;
        }
        Query query = service.query(mdl11.etObservedProperty).filter(filter);
        EntitySet streams = query.list();
        if (streams.size() > 1) {
            LOGGER.error("Found incorrect number of ObservedProperties: {} for filter: {}", streams.size(), filter);
            throw new ImportException("Found incorrect number of ObservedProperties: " + streams.size() + " for filter: " + filter);
        } else if (!streams.isEmpty()) {
            o = streams.iterator().next();
            LOGGER.debug("Found ObservedProperties {} for filter {}.", o, filter);
        }
        if (o == null) {
            LOGGER.error("Found no ObservedProperties for filter: {}.", filter);
            errorLog.addError("ObsProp not found");
        }
        cacheObsProps.put(filter, o);
        return o;
    }

}
