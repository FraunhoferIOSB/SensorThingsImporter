/*
 * Copyright (C) 2021 Fraunhofer IOSB
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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.validator;

import de.fraunhofer.iosb.ilt.sensorthingsimporter.ObservationUploader;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.FrostUtils;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.dao.BaseDao;
import de.fraunhofer.iosb.ilt.sta.model.Id;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.TimeObject;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.swe.common.Utils;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author hylke
 */
class ObsCache {

	private Id latestId;
	private Instant cacheStart;
	private final Map<TimeObject, Observation> cache = new LinkedHashMap<>();
	private final ObservationUploader uploader;
	private final boolean deleteDuplicates;

	public ObsCache(ObservationUploader uploader, boolean deleteDuplicates) {
		this.uploader = uploader;
		this.deleteDuplicates = deleteDuplicates;
	}

	public boolean isEmpty() {
		return cache.isEmpty();
	}

	public void clear() {
		latestId = null;
		cacheStart = null;
		cache.clear();
	}

	public Instant getCacheStart() {
		return cacheStart;
	}

	public void setCacheStart(Instant cacheStart) {
		this.cacheStart = cacheStart;
	}

	public boolean isBeforeStart(Instant instant) {
		return instant.isBefore(cacheStart);
	}

	public boolean clearIfDifferent(Id id) {
		if (!id.equals(latestId)) {
			clear();
			latestId = id;
			return true;
		}
		return false;
	}

	public Observation put(TimeObject time, Observation obs) {
		if (cacheStart == null) {
			cacheStart = FrostUtils.instantFrom(obs.getPhenomenonTime());
		}
		return cache.put(time, obs);
	}

	public Observation getFromCache(TimeObject checkTime, BaseDao<Observation> observations) throws ServiceFailureException {
		Instant checkInstant = FrostUtils.instantFrom(checkTime);
		List<Observation> toDelete = null;
		if (cache.isEmpty()) {
			EntityList<Observation> list = observations.query().select("@iot.id", "result", "phenomenonTime").filter("phenomenonTime ge " + checkInstant.toString()).orderBy("phenomenonTime asc").top(1000).list();
			toDelete = addToCache(list);
		} else {
			if (checkInstant.isBefore(cacheStart)) {
				EntityList<Observation> list = observations.query().select("@iot.id", "result", "phenomenonTime").filter("phenomenonTime ge " + checkInstant.toString() + " and phenomenonTime le " + cacheStart).orderBy("phenomenonTime asc").top(1000).list();
				toDelete = addToCache(list);
			}
		}
		if (!Utils.isNullOrEmpty(toDelete)) {
			uploader.delete(toDelete, 10);
		}
		return cache.get(checkTime);
	}

	public List<Observation> addToCache(EntityList<Observation> list) {
		List<Observation> toDelete = null;
		Iterator<Observation> fullIterator = list.fullIterator();
		while (fullIterator.hasNext()) {
			Observation obs = fullIterator.next();
			TimeObject phenomenonTime = obs.getPhenomenonTime();
			Observation old = cache.put(phenomenonTime, obs);
			Instant instant = FrostUtils.instantFrom(phenomenonTime);
			if (cacheStart == null || instant.isBefore(cacheStart)) {
				cacheStart = instant;
			}
			if (deleteDuplicates && old != null && !old.getId().equals(obs.getId())) {
				if (toDelete == null) {
					toDelete = new ArrayList<>();
				}
				toDelete.add(old);
			}
		}
		if (toDelete == null) {
			return Collections.emptyList();
		}
		return toDelete;
	}

}
