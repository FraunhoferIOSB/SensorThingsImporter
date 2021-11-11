/*
 * Copyright (C) 2020 Fraunhofer IOSB
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

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.Utils;
import de.fraunhofer.iosb.ilt.sta.dao.BaseDao;
import de.fraunhofer.iosb.ilt.sta.model.Entity;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.query.Query;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @param <T> The entity type this cache caches.
 * @param <U> The type of the localId.
 */
public class EntityCache<U, T extends Entity<T>> {

	private final Map<U, T> entitiesByLocalId = new LinkedHashMap<>();
	private final Map<String, T> entitiesByName = new LinkedHashMap<>();

	private final PropertyExtractor<U, T> localIdExtractor;
	private final PropertyExtractor<String, T> nameExtractor;

	public EntityCache(PropertyExtractor<U, T> localIdExtractor, PropertyExtractor<String, T> nameExtractor) {
		this.localIdExtractor = localIdExtractor;
		this.nameExtractor = nameExtractor;
	}

	public T get(U localId) {
		return entitiesByLocalId.get(localId);
	}

	public T getByName(String name) {
		return entitiesByName.get(name);
	}

	public boolean containsId(U localId) {
		return entitiesByLocalId.containsKey(localId);
	}

	public boolean isEmpty() {
		return entitiesByLocalId.isEmpty();
	}

	public int load(BaseDao<T> dao, String filter) throws ServiceFailureException {
		return load(dao, filter, "", "");
	}

	public int load(BaseDao<T> dao, String filter, String select, String expand) throws ServiceFailureException {
		Query<T> query = dao.query();
		if (!select.isEmpty()) {
			query.select(select);
		}
		if (!expand.isEmpty()) {
			query.expand(expand);
		}
		if (!Utils.isNullOrEmpty(filter)) {
			query.filter(filter);
		}
		EntityList<T> entities = query.top(10000).orderBy("id asc").list();
		Iterator<T> it = entities.fullIterator();
		int count = 0;
		while (it.hasNext()) {
			T entitiy = it.next();
			if (add(entitiy)) {
				count++;
			}
		}
		return count;
	}

	public void add(Collection<T> entities) {
		entities.stream().forEach(e -> add(e));
	}

	public boolean add(T entity) {
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
	public T registerNull(U localId) {
		return entitiesByLocalId.put(localId, null);
	}

	public Collection<T> valuesWithLocalId() {
		return entitiesByLocalId.values();
	}

	public Collection<T> valuesWithName() {
		return entitiesByName.values();
	}

	public static interface PropertyExtractor<U, T extends Entity<T>> {

		public U extractFrom(T entity);
	}

}
