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
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ObservationUploader;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.TimeObject;
import java.text.ParseException;
import net.time4j.Duration;
import net.time4j.IsoUnit;
import net.time4j.Moment;
import net.time4j.tz.ZonalOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

public class ValidatorCheckTimeInterval implements Validator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidatorCheckTimeInterval.class.getName());

    @ConfigurableField(
            label = "Duration",
            description = "The duration the phenomenonTime must have, in ISO format",
            editor = EditorString.class)
    @EditorString.EdOptsString(dflt = "PT1H")
    private String duration;

    private Duration<IsoUnit> parsedDuration;

    @Override
    public void init(ObservationUploader uploader) {
        try {
            parsedDuration = Duration.parsePeriod(duration);
        } catch (ParseException ex) {
            throw new ImportException(ex);
        }
    }

    @Override
    public boolean isValid(Observation obs) throws ImportException {
        if (parsedDuration == null) {
            throw new ImportException("No duration configured.");
        }
        TimeObject phenomenonTime = obs.getPhenomenonTime();
        if (!phenomenonTime.isInterval()) {
            return false;
        }
        Interval interval = phenomenonTime.getAsInterval();
        Moment start = Moment.from(interval.getStart());
        Moment wantedEnd = parsedDuration.addTo(start.toZonalTimestamp(ZonalOffset.UTC)).atUTC();
        if (Moment.from(interval.getEnd()).isSimultaneous(wantedEnd)) {
            return true;
        }
        LOGGER.error("Incorrect interval {}", phenomenonTime);
        return false;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

}
