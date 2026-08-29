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
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.frostclient.SensorThingsService;
import de.fraunhofer.iosb.ilt.frostclient.exception.ServiceFailureException;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.frostclient.model.ModelRegistry;
import de.fraunhofer.iosb.ilt.frostclient.model.PkValue;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11MultiDatastream;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ErrorLog;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A datastream mapper that always returns the same Datastream.
 */
public class DsMapperFixed implements DatastreamMapper {

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(DsMapperFixed.class);

    private SensorThingsService service;
    private SensorThingsV11Sensing mdl11;
    private SensorThingsV11MultiDatastream mdlMds;

    private Entity ds;
    private Entity mds;

    @ConfigurableField(editor = EditorInt.class, label = "Datastream ID", description = "The datastream id to add the observations to.")
    @EditorInt.EdOptsInt()
    private int dsId;

    public DsMapperFixed() {
        this.ds = null;
    }

    @Override
    public void init(SensorThingsService service) throws ImportException {
        this.service = service;
        final ModelRegistry mr = service.getModelRegistry();
        mdl11 = mr.getModel(SensorThingsV11Sensing.class);
        mdlMds = mr.getModel(SensorThingsV11MultiDatastream.class);
    }

    private void init(boolean multi) {
        try {
            if (multi) {
                mds = service.dao(mdlMds.etMultiDatastream).find(PkValue.of(dsId));
                LOGGER.info("Using fixed multiDatastream: {}", mds);
            } else {
                ds = service.dao(mdl11.etDatastream).find(PkValue.of(dsId));
                LOGGER.info("Using fixed datatsream: {}", ds);
            }
        } catch (ServiceFailureException exc) {
            throw new IllegalArgumentException("Could not fetch (multi)datastream for id " + dsId, exc);
        }
    }

    @Override
    public Entity getDatastreamFor(CSVRecord record, ErrorLog errorLog) {
        if (ds == null) {
            init(false);
        }
        return ds;
    }

    @Override
    public Entity getMultiDatastreamFor(CSVRecord record, ErrorLog errorLog) {
        if (mds == null) {
            init(true);
        }
        return mds;
    }

}
