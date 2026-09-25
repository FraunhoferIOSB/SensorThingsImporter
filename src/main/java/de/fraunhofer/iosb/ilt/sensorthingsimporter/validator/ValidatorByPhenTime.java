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

import static de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing.EP_PHENOMENONTIME;
import static de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing.EP_RESULT;

import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.frostclient.SensorThingsService;
import de.fraunhofer.iosb.ilt.frostclient.dao.Dao;
import de.fraunhofer.iosb.ilt.frostclient.exception.ServiceFailureException;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.frostclient.model.ModelRegistry;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11MultiDatastream;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.TimeValue;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ObservationUploader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates Observations by searching in the service by phenomenonTime.
 */
public class ValidatorByPhenTime implements Validator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidatorByPhenTime.class);

    @ConfigurableField(editor = EditorBoolean.class,
            label = "Update", description = "Update results that are different.")
    @EditorBoolean.EdOptsBool()
    private boolean update;

    @ConfigurableField(editor = EditorBoolean.class,
            label = "Cache", description = "Download & cache all observations with phenomenonTime later than the first encoutered.")
    @EditorBoolean.EdOptsBool()
    private boolean cacheObservations;

    @ConfigurableField(editor = EditorBoolean.class,
            label = "Cache deletes duplicates", description = "Delete duplicates if the cache encouters them.")
    @EditorBoolean.EdOptsBool(dflt = true)
    private boolean deleteDuplicates;

    private ObservationUploader uploader;

    private final ThreadLocal<ObsCache> cacheHolder = new ThreadLocal<>();
    private SensorThingsV11Sensing mdl11;
    private SensorThingsV11MultiDatastream mdlMds;

    @Override
    public void init(ObservationUploader uploader) {
        this.uploader = uploader;
        try {
            final SensorThingsService service = uploader.getService();
            final ModelRegistry mr = service.getModelRegistry();
            mdl11 = mr.getModel(SensorThingsV11Sensing.class);
            mdlMds = mr.getModel(SensorThingsV11MultiDatastream.class);
        } catch (MalformedURLException ex) {
            throw new ImportException(ex);
        }
    }

    private ObsCache getCache() {
        ObsCache cache = cacheHolder.get();
        if (cache == null) {
            cache = new ObsCache(uploader, deleteDuplicates);
            cacheHolder.set(cache);
        }
        return cache;
    }

    private boolean resultCompare(Object one, Object two) {
        if (one == null) {
            return two == null;
        }
        if (two == null) {
            return false;
        }
        if (one.equals(two)) {
            return true;
        }
        if (one instanceof List) {
            if (!(two instanceof List)) {
                return false;
            }
            List listOne = (List) one;
            List listTwo = (List) two;
            int size = listOne.size();
            if (listTwo.size() != size) {
                return false;
            }
            for (int i = 0; i < size; i++) {
                if (!resultCompare(listOne.get(i), listTwo.get(i))) {
                    return false;
                }
            }
            return true;
        }
        try {
            if (one instanceof Long && two instanceof Integer) {
                return one.equals(Long.valueOf((Integer) two));
            }
            if (two instanceof Long && one instanceof Integer) {
                return two.equals(Long.valueOf((Integer) one));
            }
            if (one instanceof BigDecimal && two instanceof BigDecimal) {
                // Would have returned true above if equal
                return false;
            }
            if (one instanceof BigDecimal) {
                return ((Comparable<BigDecimal>) one).compareTo(new BigDecimal(two.toString())) == 0;
            }
            if (two instanceof BigDecimal) {
                return ((Comparable<BigDecimal>) two).compareTo(new BigDecimal(one.toString())) == 0;
            }
        } catch (NumberFormatException e) {
            LOGGER.trace("Not both bigdecimal.", e);
            // not both bigDecimal.
        }
        return false;
    }

    private Dao validateCache(Entity ds, Entity mds) {
        if (cacheObservations) {
            if (ds != null) {
                getCache().clearIfDifferent(ds.getPrimaryKeyValues());
            }
            if (mds != null) {
                getCache().clearIfDifferent(mds.getPrimaryKeyValues());
            }
        }
        if (ds != null) {
            return ds.dao(mdl11.npDatastreamObservations);
        }
        if (mds != null) {
            return mds.dao(mdlMds.npMultidatastreamObservations);
        }
        throw new IllegalArgumentException("Must pass either a Datastream or multiDatastream.");
    }

    private Entity getObservation(TimeValue phenTime, Dao observations) throws ServiceFailureException {
        if (cacheObservations) {
            return getCache().getFromCache(phenTime, observations);
        }
        return observations.query().select("@iot.id", "result").filter("phenomenonTime eq " + phenTime.toString()).first();
    }

    private void addToCache(Entity obs) {
        if (cacheObservations) {
            getCache().put(obs.getProperty(EP_PHENOMENONTIME), obs);
        }
    }

    @Override
    public boolean isValid(Entity obs) throws ImportException {
        try {
            Entity ds = obs.getProperty(mdl11.npObservationDatastream);
            Entity mds = null;
            if (mdlMds != null) {
                mds = obs.getProperty(mdlMds.npObservationMultidatastream);
            }
            Dao observations = validateCache(ds, mds);

            TimeValue phenomenonTime = obs.getProperty(EP_PHENOMENONTIME);
            Entity first = getObservation(phenomenonTime, observations);
            if (first == null) {
                addToCache(obs);
                return true;
            } else {
                final Object newResult = obs.getProperty(EP_RESULT);
                final Object existingResult = first.getProperty(EP_RESULT);
                if (!resultCompare(newResult, existingResult)) {
                    LOGGER.debug("Observation {} with given phenomenonTime {} exists, but result not the same. {} != {} .", first, phenomenonTime, newResult, existingResult);
                    if (update) {
                        obs.setPrimaryKeyValues(first.getPrimaryKeyValues());
                        addToCache(obs);
                        return true;
                    }
                }
                return false;
            }
        } catch (ServiceFailureException ex) {
            LOGGER.debug("Exception fetching validation observations: {}", ex.getMessage());
            throw new ImportException("Failed to validate.", ex);
        }
    }

}
