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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.timegen;

import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableClass;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import java.time.Instant;
import net.time4j.Moment;

@ConfigurableClass
public interface TimeGen extends AnnotatedConfigurable<Object, Object> {

    public Instant getInstant();

    public Instant getInstantFromDs(Entity ds);

    public Instant getInstantFromMds(Entity mds);

    public default Moment getMoment() {
        return Moment.from(getInstant());
    }

    public default Moment getMomentFromDs(Entity ds) {
        return Moment.from(getInstantFromDs(ds));
    }

    public default Moment getMomentFromMds(Entity mds) {
        return Moment.from(getInstantFromMds(mds));
    }
}
