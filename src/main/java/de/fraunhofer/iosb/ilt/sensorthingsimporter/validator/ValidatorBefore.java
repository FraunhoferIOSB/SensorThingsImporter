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

import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.TimeValue;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ObservationUploader;
import java.util.concurrent.TimeUnit;
import net.time4j.Moment;

/**
 * Checks if the observation has a phenomenonTime that is later than the latest
 * in the configured datastream.
 */
public class ValidatorBefore implements Validator {

    @ConfigurableField(editor = EditorInt.class,
            label = "days", description = "The number of days before now.")
    @EditorInt.EdOptsInt(min = 0, max = 999999, step = 1, dflt = 1)
    private int days;

    @ConfigurableField(editor = EditorInt.class,
            label = "minutes", description = "The number of minutes before now.")
    @EditorInt.EdOptsInt(min = 0, max = 999999, step = 1, dflt = 0)
    private int minutes;

    private Moment refTime;

    @Override
    public boolean isValid(Entity obs) throws ImportException {
        TimeValue phenomenonTime = obs.getProperty(SensorThingsV11Sensing.EP_PHENOMENONTIME);
        Moment obsInstant;
        if (phenomenonTime.isInterval()) {
            obsInstant = phenomenonTime.getInterval().getStart();
        } else {
            obsInstant = phenomenonTime.getInstant().getDateTime();
        }
        return refTime.isAfter(obsInstant);
    }

    @Override
    public void init(ObservationUploader uploader) {
        Moment now = Moment.nowInSystemTime();
        refTime = now.minus(days, TimeUnit.DAYS);
        refTime = refTime.minus(minutes, TimeUnit.MINUTES);
    }

}
