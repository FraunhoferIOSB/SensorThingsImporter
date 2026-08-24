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
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ObservationUploader;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.TimeObject;
import java.time.Instant;
import org.threeten.extra.Days;
import org.threeten.extra.Minutes;

/**
 * Checks if the observation has a phenomenonTime that is later than the latest
 * in the configured datastream.
 */
public class ValidatorAfter implements Validator {

    @ConfigurableField(editor = EditorInt.class,
            label = "days", description = "The number of days before now.")
    @EditorInt.EdOptsInt(min = 0, max = 999999, step = 1, dflt = 1)
    private int days;

    @ConfigurableField(editor = EditorInt.class,
            label = "minutes", description = "The number of minutes before now.")
    @EditorInt.EdOptsInt(min = 0, max = 999999, step = 1, dflt = 0)
    private int minutes;

    private Instant refTime;

    @Override
    public boolean isValid(Observation obs) throws ImportException {
        TimeObject phenomenonTime = obs.getPhenomenonTime();
        Instant obsInstant;
        if (phenomenonTime.isInterval()) {
            obsInstant = phenomenonTime.getAsInterval().getStart();
        } else {
            obsInstant = phenomenonTime.getAsDateTime().toInstant();
        }
        return refTime.isBefore(obsInstant);
    }

    @Override
    public void init(ObservationUploader uploader) {
        Instant now = Instant.now();
        refTime = now.minus(Days.of(days));
        refTime = refTime.minus(Minutes.of(minutes));
    }

}
