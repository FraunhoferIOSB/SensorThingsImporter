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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.records;

import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.frostclient.SensorThingsService;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ErrorLog;

/**
 * Finds Datastreams based on a Tuple.
 */
public interface DatastreamMapper extends AnnotatedConfigurable<Object, Object> {

    public default void init(SensorThingsService service) throws ImportException {
        // does nothing by default.
    }

    /**
     * Get the Datastream to be used for the given record.
     *
     * @param record The record to get the Datastream for.
     * @param errorLog The error logger to log non-fatal errors to.
     * @return The Datastream to use for the given record.
     * @throws ImportException if there is a permanent failure.
     */
    public Entity getDatastreamFor(Tuple record, ErrorLog errorLog) throws ImportException;

    public Entity getMultiDatastreamFor(Tuple record, ErrorLog errorLog) throws ImportException;
}
