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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.utils;

import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.frostclient.model.EntityType;
import de.fraunhofer.iosb.ilt.frostclient.model.Property;
import de.fraunhofer.iosb.ilt.frostclient.model.property.type.TypeComplex;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.MapValue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 *
 * @author hylke
 */
public class CollectionsHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(CollectionsHelper.class);

    private CollectionsHelper() {
        // Utility class
    }

    public static void setOn(final Map<String, Object> map, final String path, final Object value) {
        setOn(map, Arrays.asList(StringUtils.split(path, '/')), value);
    }

    public static void setOn(final Map<String, Object> map, final List<String> path, final Object value) {
        setOn(map, path, 0, value);
    }

    public static void setOn(final Map<String, Object> map, final List<String> path, final int idx, final Object value) {
        final String key = path.get(idx);
        if (idx == path.size() - 1) {
            map.put(key, value);
            return;
        }
        Object subEntry = map.get(key);
        if (subEntry == null) {
            Map<String, Object> subMap = new HashMap<>();
            map.put(key, subMap);
            setOn(subMap, path, idx + 1, value);
            return;
        }
        if (subEntry instanceof Map) {
            setOn((Map) subEntry, path, idx + 1, value);
            return;
        }
        if (subEntry instanceof List) {
            throw new IllegalArgumentException("Item at path element " + key + " is a list.");
        }
        throw new IllegalArgumentException("Element at path index " + idx + " is not a map or list.");
    }

    public static Object getFrom(final List<Object> list, final List<String> path) {
        return getFrom((Object) list, path);
    }

    public static Object getFrom(final Map<String, Object> map, final String... path) {
        return getFrom((Object) map, Arrays.asList(path));
    }

    public static Object getFrom(final Map<String, Object> map, final List<String> path) {
        return getFrom((Object) map, path);
    }

    public static Object getFrom(final Object mapOrList, final String path) {
        String[] pathItems = StringUtils.split(path, '/');
        return getFrom(mapOrList, Arrays.asList(pathItems));
    }

    public static Object getFrom(final Object mapOrList, final List<String> path) {
        Object currentEntry = mapOrList;
        int last = path.size();
        for (int idx = 0; idx < last; idx++) {
            if (currentEntry == null) {
                return null;
            }
            String key = path.get(idx);
            switch (currentEntry) {
                case Map map ->
                    currentEntry = map.get(key);
                case ObjectNode on ->
                    currentEntry = on.get(key);
                case ArrayNode an ->
                    currentEntry = an.get(Integer.parseInt(key));
                case Entity e -> {
                    EntityType et = e.getType();
                    Property p = et.getProperty(key);
                    if (p == null) {
                        LOGGER.debug("No property {} on {}", key, e);
                        return null;
                    }
                    currentEntry = e.getProperty(p);
                }
                case MapValue mv -> {
                    TypeComplex tc = mv.getType();
                    currentEntry = mv.getProperty(key);
                }
                case List list -> {
                    try {
                        currentEntry = list.get(Integer.parseInt(key));
                    } catch (NumberFormatException | IndexOutOfBoundsException ex) {
                        LOGGER.warn("Failed to get {} from {}.", key, currentEntry, ex);
                        return null;
                    }
                }
                default -> {
                }
            }
        }
        return currentEntry;
    }
}
