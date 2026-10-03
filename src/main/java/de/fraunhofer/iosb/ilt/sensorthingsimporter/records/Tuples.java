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

import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.CollectionsHelper;
import java.util.Objects;
import tools.jackson.databind.node.ValueNode;

/**
 * Various tuple types.
 */
public class Tuples {

    private Tuples() {
        // Not for instantiation.
    }

    public static class EntityTuple implements Tuple {

        private final Entity entity;

        public EntityTuple(Entity entity) {
            this.entity = entity;
        }

        @Override
        public String getString(int idx) {
            throw new UnsupportedOperationException("Integer indexes not supported on Entities.");
        }

        @Override
        public String getObject(int idx) {
            throw new UnsupportedOperationException("Integer indexes not supported on Entities.");
        }

        @Override
        public String getString(String path) {
            final Object object = getObject(path);
            if (object instanceof ValueNode vn) {
                return vn.asString();
            }
            return Objects.toString(object, null);
        }

        @Override
        public Object getObject(String path) {
            return CollectionsHelper.getFrom(entity, path);
        }

        @Override
        public boolean isMapped(String path) {
            return getObject(path) != null;
        }

        public static EntityTuple of(Entity entity) {
            return new EntityTuple(entity);
        }
    }
}
