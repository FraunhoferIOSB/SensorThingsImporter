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

import de.fraunhofer.iosb.ilt.frostclient.dao.Dao;
import de.fraunhofer.iosb.ilt.frostclient.exception.ServiceFailureException;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.frostclient.model.EntitySet;
import de.fraunhofer.iosb.ilt.frostclient.query.Query;
import de.fraunhofer.iosb.ilt.frostclient.utils.StringHelper;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @param <U> The type of the localId.
 */
public class EntityCache<U> {

    private final Map<U, Entity> entitiesByLocalId = new LinkedHashMap<>();
    private final Map<String, Entity> entitiesByName = new LinkedHashMap<>();

    private final PropertyExtractor<U> localIdExtractor;
    private final PropertyExtractor<String> nameExtractor;

    public EntityCache(PropertyExtractor<U> localIdExtractor, PropertyExtractor<String> nameExtractor) {
        this.localIdExtractor = localIdExtractor;
        this.nameExtractor = nameExtractor;
    }

    public Entity get(U localId) {
        return entitiesByLocalId.get(localId);
    }

    public Entity getByName(String name) {
        return entitiesByName.get(name);
    }

    public boolean containsId(U localId) {
        return entitiesByLocalId.containsKey(localId);
    }

    public boolean isEmpty() {
        return entitiesByLocalId.isEmpty();
    }

    public int load(Dao dao, String filter) throws ServiceFailureException {
        return load(dao, filter, "", "");
    }

    public int load(Dao dao, String filter, String select, String expand) throws ServiceFailureException {
        Query query = dao.query();
        if (!select.isEmpty()) {
            query.select(select);
        }
        if (!expand.isEmpty()) {
            query.expand(expand);
        }
        if (!StringHelper.isNullOrEmpty(filter)) {
            query.filter(filter);
        }
        EntitySet entities = query.top(10000).orderBy("id asc").list();
        Iterator<Entity> it = entities.iterator();
        int count = 0;
        while (it.hasNext()) {
            Entity entitiy = it.next();
            if (add(entitiy)) {
                count++;
            }
        }
        return count;
    }

    public void add(Collection<Entity> entities) {
        entities.stream().forEach(e -> add(e));
    }

    public boolean add(Entity entity) {
        boolean hasLocalId = false;
        try {
            U localId = localIdExtractor.extractFrom(entity);
            if (localId != null) {
                entitiesByLocalId.put(localId, entity);
                hasLocalId = true;
            }
        } catch (RuntimeException ex) {
            // probably no localId, ignore.
        }
        if (nameExtractor != null) {
            String name = nameExtractor.extractFrom(entity);
            entitiesByName.put(name, entity);
        }
        return hasLocalId;
    }

    /**
     * Register a NULL value for the given localId. This can be used to cache
     * the fact that the given localId does not have an Entity.
     *
     * @param localId The localId to cache.
     * @return the old value for the given localId, or null if there was no old
     * value registered.
     */
    public Entity registerNull(U localId) {
        return entitiesByLocalId.put(localId, null);
    }

    public Collection<Entity> valuesWithLocalId() {
        return entitiesByLocalId.values();
    }

    public Collection<Entity> valuesWithName() {
        return entitiesByName.values();
    }

    public static interface PropertyExtractor<U> {

        public U extractFrom(Entity entity);
    }

}
