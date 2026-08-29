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
package de.fraunhofer.iosb.ilt.sensorthingsimporter;

import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.frostclient.SensorThingsService;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ProgressTracker;
import java.util.List;

/**
 * The main importer interface.
 */
public interface Importer extends Iterable<List<Entity>>, AnnotatedConfigurable<Object, Object> {

    public default void init(SensorThingsService service) throws ImportException {
    }

    public default void setNoAct(boolean noAct) {
        // does nothing by default
    }

    public default void setProgressTracker(ProgressTracker tracker) {
        // does nothing by default
    }

    public default String getErrorLog() {
        return "";
    }

    public default int getErrorCount() {
        return 0;
    }
}
