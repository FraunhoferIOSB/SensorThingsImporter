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

import de.fraunhofer.iosb.ilt.frostclient.SensorThingsService;
import de.fraunhofer.iosb.ilt.frostclient.exception.ServiceFailureException;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.frostclient.model.ModelRegistry;
import de.fraunhofer.iosb.ilt.frostclient.model.PkValue;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11MultiDatastream;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.TimeValue;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ObservationUploader;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import net.time4j.Moment;

/**
 * Checks if the observation has a phenomenonTime that is later than the latest
 * in the configured datastream.
 */
public class ValidatorNewer implements Validator {

    private static final Moment MOMENT_MIN = Moment.axis().getMinimum();

    private final Map<PkValue, Moment> datastreamCache = new HashMap<>();
    private final Map<PkValue, Moment> multiDatastreamCache = new HashMap<>();

    private SensorThingsV11Sensing mdl11;
    private SensorThingsV11MultiDatastream mdlMds;

    @Override
    public void init(ObservationUploader uploader) {
        try {
            final SensorThingsService service = uploader.getService();
            final ModelRegistry mr = service.getModelRegistry();
            mdl11 = mr.getModel(SensorThingsV11Sensing.class);
            mdlMds = mr.getModel(SensorThingsV11MultiDatastream.class);
        } catch (MalformedURLException ex) {
            throw new ImportException(ex);
        }
    }

    @Override
    public boolean isValid(Entity obs) throws ImportException {
        try {
            Moment latest;
            Entity ds = obs.getProperty(mdl11.npObservationDatastream);
            if (ds == null) {
                Entity mds = obs.getProperty(mdlMds.npObservationMultidatastream);
                if (mds == null) {
                    throw new ImportException("Observation has no Datastream of Multidatastream set!");
                }
                latest = getTimeForMultiDatastream(mds);
            } else {
                latest = getTimeForDatastream(ds);
            }
            TimeValue phenomenonTime = obs.getProperty(EP_PHENOMENONTIME);
            Moment obsInstant;
            if (phenomenonTime.isInterval()) {
                obsInstant = phenomenonTime.getInterval().getStart();
            } else {
                obsInstant = phenomenonTime.getInstant().getDateTime();
            }
            return latest.isBefore(obsInstant);
        } catch (ServiceFailureException ex) {
            throw new ImportException("Failed to validate.", ex);
        }
    }

    private Moment getTimeForDatastream(Entity ds) throws ServiceFailureException {
        PkValue dsId = ds.getPrimaryKeyValues();
        Moment latest = datastreamCache.get(dsId);
        if (latest == null) {
            Entity firstObs = ds.query(mdl11.npDatastreamObservations)
                    .select("@iot.id", "phenomenonTime")
                    .orderBy("phenomenonTime desc")
                    .first();
            if (firstObs == null) {
                latest = MOMENT_MIN;
            } else {
                TimeValue phenomenonTime = firstObs.getProperty(EP_PHENOMENONTIME);
                if (phenomenonTime.isInterval()) {
                    latest = phenomenonTime.getInterval().getStart();
                } else {
                    latest = phenomenonTime.getInstant().getDateTime();
                }
            }
            datastreamCache.put(dsId, latest);
        }
        return latest;
    }

    private Moment getTimeForMultiDatastream(Entity mds) throws ServiceFailureException {
        PkValue dsId = mds.getPrimaryKeyValues();
        Moment latest = multiDatastreamCache.get(dsId);
        if (latest == null) {
            Entity firstObs = mds.query(mdlMds.npMultidatastreamObservations)
                    .select("@iot.id", "phenomenonTime")
                    .orderBy("phenomenonTime desc")
                    .first();
            if (firstObs == null) {
                latest = MOMENT_MIN;
            } else {
                TimeValue phenomenonTime = firstObs.getProperty(EP_PHENOMENONTIME);
                if (phenomenonTime.isInterval()) {
                    latest = phenomenonTime.getInterval().getStart();
                } else {
                    latest = phenomenonTime.getInstant().getDateTime();
                }
            }
            multiDatastreamCache.put(dsId, latest);
        }
        return latest;
    }
}
