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

import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.frostclient.SensorThingsService;
import de.fraunhofer.iosb.ilt.frostclient.exception.ServiceFailureException;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.frostclient.model.EntitySet;
import de.fraunhofer.iosb.ilt.frostclient.model.ModelRegistry;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11MultiDatastream;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing;
import de.fraunhofer.iosb.ilt.frostclient.query.Query;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ErrorLog;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator.StringType;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds Datastrams based on a STA filter query.
 */
public class DsMapperFilter implements DatastreamMapper {

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(DsMapperFilter.class);
    private final Map<String, Entity> datastreamCache = new HashMap<>();
    private final Map<String, Entity> multiDatastreamCache = new HashMap<>();

    @ConfigurableField(editor = EditorString.class,
            label = "Filter", description = "A filter that will be added to the query for the datastream.\nUse placeholders {colNr} to add the content of columns to the query.")
    @EditorString.EdOptsString(dflt = "Thing/properties/id eq {1}", lines = 3)
    private String filterTemplate;

    @ConfigurableField(editor = EditorSubclass.class, optional = true,
            label = "DS Generator", description = "Generates Datastreams if the required one does not exist yet.")
    @EditorSubclass.EdOptsSubclass(iface = DatastreamGenerator.class)
    private DatastreamGenerator dsGenerator;

    private SensorThingsService service;
    private SensorThingsV11Sensing mdl11;
    private SensorThingsV11MultiDatastream mdlMds;

    public DsMapperFilter() {
    }

    @Override
    public void init(SensorThingsService service) throws ImportException {
        this.service = service;
        final ModelRegistry mr = service.getModelRegistry();
        mdl11 = mr.getModel(SensorThingsV11Sensing.class);
        mdlMds = mr.getModel(SensorThingsV11MultiDatastream.class);
        if (dsGenerator != null) {
            dsGenerator.init(service);
        }
    }

    @Override
    public Entity getDatastreamFor(CSVRecord record, ErrorLog errorLog) throws ImportException {
        try {
            String filter = Translator.fillTemplate(filterTemplate, record, StringType.URL, true);
            Entity ds = getDatastreamFor(filter, record, errorLog);
            return ds;
        } catch (ServiceFailureException ex) {
            LOGGER.error("Failed to fetch datastream.", ex);
            throw new IllegalArgumentException(ex);
        }
    }

    @Override
    public Entity getMultiDatastreamFor(CSVRecord record, ErrorLog errorLog) {
        try {
            String filter = Translator.fillTemplate(filterTemplate, record, StringType.URL, true);
            Entity ds = getMultiDatastreamFor(filter, record, errorLog);
            return ds;
        } catch (ServiceFailureException ex) {
            LOGGER.error("Failed to fetch datastream.", ex);
            throw new IllegalArgumentException(ex);
        }
    }

    private Entity getDatastreamFor(String filter, CSVRecord record, ErrorLog errorLog) throws ServiceFailureException, ImportException {
        Entity ds = datastreamCache.get(filter);
        if (ds != null) {
            return ds;
        }
        if (datastreamCache.containsKey(filter)) {
            // We previously had found nothing. Don't search again.
            return null;
        }
        Query query = service.query(mdl11.etDatastream).filter(filter);
        EntitySet streams = query.list();
        if (streams.size() > 1) {
            LOGGER.error("Found incorrect number of datastreams: {} for filter: {}", streams.size(), filter);
            if (dsGenerator != null) {
                ds = dsGenerator.createDatastreamFor(record, errorLog);
                if (ds == null) {
                    LOGGER.info("DsGenerator did not fix it.");
                }
                return ds;
            }
            return null;
        } else if (streams.isEmpty()) {
            if (dsGenerator != null) {
                ds = dsGenerator.createDatastreamFor(record, errorLog);
                if (ds != null) {
                    LOGGER.info("Created datastream {} for filter {}.", ds, filter);
                }
            }
        } else {
            ds = streams.iterator().next();
            LOGGER.debug("Found datastream {} for filter {}.", ds, filter);
        }
        if (ds == null) {
            errorLog.addError("DS not found");
            LOGGER.error("Found no datastreams for filter: {}.", filter);
        }
        datastreamCache.put(filter, ds);
        return ds;
    }

    private Entity getMultiDatastreamFor(String filter, CSVRecord record, ErrorLog errorLog) throws ServiceFailureException {
        Entity mds = multiDatastreamCache.get(filter);
        if (mds != null) {
            return mds;
        }
        Query query = service.query(mdlMds.etMultiDatastream).filter(filter);
        EntitySet streams = query.list();
        if (streams.size() != 1) {
            LOGGER.error("Found incorrect number of multiDatastreams: {}", streams.size());
            throw new IllegalArgumentException("Found incorrect number of multiDatastreams: " + streams.size());
        }
        mds = streams.iterator().next();
        LOGGER.info("Found multiDatastreams {} for query {}.", mds, filter);
        multiDatastreamCache.put(filter, mds);
        return mds;
    }

}
