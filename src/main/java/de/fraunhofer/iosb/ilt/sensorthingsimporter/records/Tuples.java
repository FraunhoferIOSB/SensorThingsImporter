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
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.node.ValueNode;

/**
 * Various tuple types.
 */
public class Tuples {

    private Tuples() {
        // Not for instantiation.
    }

    public static final class JsonTuple implements Tuple {

        private final ObjectNode item;

        public JsonTuple(ObjectNode item) {
            this.item = item;
        }

        @Override
        public String getString(int idx) {
            throw new UnsupportedOperationException("Fetching by index not supported.");
        }

        @Override
        public Object getObject(int idx) {
            throw new UnsupportedOperationException("Fetching by index not supported.");
        }

        @Override
        public String getString(String name) {
            Object object = getObject(name);
            if (object instanceof ValueNode vn) {
                return vn.asString();
            }
            return Objects.toString(object);
        }

        @Override
        public Object getObject(String name) {
            return CollectionsHelper.getFrom(item, name);
        }

        @Override
        public boolean isMapped(String name) {
            return CollectionsHelper.getFrom(item, name) != null;
        }

        @Override
        public Object getSource() {
            return item;
        }

        public JsonTuple of(ObjectNode item) {
            return new JsonTuple(item);
        }
    }

    public static final class CsvTuple implements Tuple {

        private final boolean stripNulls;
        private final CSVRecord record;

        public CsvTuple(CSVRecord record) {
            this(record, false);
        }

        public CsvTuple(CSVRecord record, boolean stripNulls) {
            this.stripNulls = stripNulls;
            this.record = record;
        }

        @Override
        public String getString(String name) {
            if (stripNulls) {
                return StringUtils.replaceChars(record.get(name), "\u0000", "");
            }
            return record.get(name);
        }

        @Override
        public Object getObject(String name) {
            return getString(name);
        }

        @Override
        public String getString(int idx) {
            if (stripNulls) {
                return StringUtils.replaceChars(record.get(idx), "\u0000", "");
            }
            return record.get(idx);
        }

        @Override
        public Object getObject(int idx) {
            return getString(idx);
        }

        @Override
        public boolean isMapped(String name) {
            return record.isMapped(name);
        }

        @Override
        public Object getSource() {
            return record;
        }

        public static CsvTuple of(CSVRecord record) {
            return new CsvTuple(record);
        }

        public static CsvTuple of(CSVRecord record, boolean stripNulls) {
            return new CsvTuple(record, stripNulls);
        }
    }

    public static final class EntityTuple implements Tuple {

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

        @Override
        public Object getSource() {
            return entity;
        }

        public static EntityTuple of(Entity entity) {
            return new EntityTuple(entity);
        }
    }
}
