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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.validator;

import de.fraunhofer.iosb.ilt.frostclient.dao.Dao;
import de.fraunhofer.iosb.ilt.frostclient.exception.ServiceFailureException;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.frostclient.model.EntitySet;
import de.fraunhofer.iosb.ilt.frostclient.model.PkValue;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.TimeValue;
import de.fraunhofer.iosb.ilt.frostclient.utils.StringHelper;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ObservationUploader;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.FrostUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.time4j.Moment;

class ObsCache {

    private PkValue latestId;
    private Moment cacheStart;
    private final Map<TimeValue, Entity> cache = new LinkedHashMap<>();
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

    public Moment getCacheStart() {
        return cacheStart;
    }

    public void setCacheStart(Moment cacheStart) {
        this.cacheStart = cacheStart;
    }

    public boolean isBeforeStart(Moment instant) {
        return instant.isBefore(cacheStart);
    }

    public boolean clearIfDifferent(PkValue id) {
        if (!id.equals(latestId)) {
            clear();
            latestId = id;
            return true;
        }
        return false;
    }

    public Entity put(TimeValue time, Entity obs) {
        if (cacheStart == null) {
            cacheStart = FrostUtils.instantFrom(obs.getProperty(SensorThingsV11Sensing.EP_PHENOMENONTIME));
        }
        return cache.put(time, obs);
    }

    public Entity getFromCache(TimeValue checkTime, Dao observations) throws ServiceFailureException {
        Moment checkInstant = FrostUtils.instantFrom(checkTime);
        List<Entity> toDelete = null;
        if (cache.isEmpty()) {
            EntitySet list = observations.query()
                    .select("@iot.id", "result", "phenomenonTime")
                    .filter("phenomenonTime ge " + checkInstant.toString())
                    .orderBy("phenomenonTime asc")
                    .top(100000)
                    .list();
            toDelete = addToCache(list);
        } else {
            if (checkInstant.isBefore(cacheStart)) {
                EntitySet list = observations.query()
                        .select("@iot.id", "result", "phenomenonTime")
                        .filter("phenomenonTime ge " + checkInstant.toString() + " and phenomenonTime le " + cacheStart)
                        .orderBy("phenomenonTime asc")
                        .top(100000)
                        .list();
                toDelete = addToCache(list);
            }
        }
        if (!StringHelper.isNullOrEmpty(toDelete)) {
            uploader.delete(toDelete, 10);
        }
        return cache.get(checkTime);
    }

    public List<Entity> addToCache(EntitySet list) {
        List<Entity> toDelete = null;
        Iterator<Entity> fullIterator = list.iterator();
        while (fullIterator.hasNext()) {
            Entity obs = fullIterator.next();
            TimeValue phenomenonTime = obs.getProperty(SensorThingsV11Sensing.EP_PHENOMENONTIME);
            Entity old = cache.put(phenomenonTime, obs);
            Moment instant = FrostUtils.instantFrom(phenomenonTime);
            if (cacheStart == null || instant.isBefore(cacheStart)) {
                cacheStart = instant;
            }
            if (deleteDuplicates && old != null && !old.getPrimaryKeyValues().equals(obs.getPrimaryKeyValues())) {
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
