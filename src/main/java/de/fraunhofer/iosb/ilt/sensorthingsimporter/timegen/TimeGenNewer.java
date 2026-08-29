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

import static de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing.EP_PHENOMENONTIME;
import static de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing.EP_PHENOMENONTIMEDS;

import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.frostclient.SensorThingsService;
import de.fraunhofer.iosb.ilt.frostclient.exception.ServiceFailureException;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11MultiDatastream;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.TimeInterval;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.TimeValue;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;
import net.time4j.Moment;
import org.slf4j.LoggerFactory;

public class TimeGenNewer implements TimeGen {

    /**
     * The logger for this class.
     */
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TimeGenNewer.class);

    @ConfigurableField(editor = EditorString.class,
            label = "StartTime", description = "The starting time, if the datastream has no observations yet.")
    @EditorString.EdOptsString(dflt = "2017-01-01T00:00:00Z")
    private String startTime;

    private SensorThingsV11Sensing mdl11;
    private SensorThingsV11MultiDatastream mdlMds;

    @Override
    public Instant getInstant() {
        return ZonedDateTime.parse(startTime).toInstant();
    }

    @Override
    public Instant getInstantFromDs(Entity ds) {
        return getMomentFromDs(ds).toTemporalAccessor();
    }

    @Override
    public Moment getMomentFromDs(Entity ds) {

        TimeInterval phenomenonTimeDs = ds.getProperty(EP_PHENOMENONTIMEDS);
        if (phenomenonTimeDs != null) {
            return phenomenonTimeDs.getEnd().plus(1, TimeUnit.SECONDS);
        }

        if (mdl11 == null) {
            SensorThingsService service = ds.getService();
            mdl11 = service.getModelRegistry().getModel(SensorThingsV11Sensing.class);
        }
        if (!ds.getProperty(mdl11.npDatastreamObservations, false).isEmpty()) {
            TimeValue phenomenonTime = ds.getProperty(mdl11.npDatastreamObservations, false).toList().get(0).getProperty(EP_PHENOMENONTIME);
            if (phenomenonTime.isInterval()) {
                return phenomenonTime.getInterval().getEnd();
            }
            return phenomenonTime.getInstant().getDateTime().plus(1, TimeUnit.SECONDS);
        }
        try {
            Entity firstObs = ds.dao(mdl11.npDatastreamObservations).query().top(1).orderBy("phenomenonTime desc").first();
            if (firstObs != null) {
                TimeValue phenomenonTime = firstObs.getProperty(EP_PHENOMENONTIME);
                if (phenomenonTime.isInterval()) {
                    return phenomenonTime.getInterval().getEnd();
                }
                return phenomenonTime.getInstant().getDateTime().plus(1, TimeUnit.SECONDS);
            }
        } catch (ServiceFailureException ex) {
            LOGGER.error("Failed to fetch last Observation.", ex);
        }
        return Moment.from(ZonedDateTime.parse(startTime).toInstant());
    }

    @Override
    public Instant getInstantFromMds(Entity mds) {
        return getMomentFromMds(mds).toTemporalAccessor();
    }

    @Override
    public Moment getMomentFromMds(Entity mds) {
        TimeInterval phenomenonTimeMds = mds.getProperty(EP_PHENOMENONTIMEDS);
        if (phenomenonTimeMds != null) {
            return phenomenonTimeMds.getEnd();
        }

        if (mdlMds == null) {
            SensorThingsService service = mds.getService();
            mdlMds = service.getModelRegistry().getModel(SensorThingsV11MultiDatastream.class);
        }

        return Moment.from(ZonedDateTime.parse(startTime).toInstant());
    }

}
