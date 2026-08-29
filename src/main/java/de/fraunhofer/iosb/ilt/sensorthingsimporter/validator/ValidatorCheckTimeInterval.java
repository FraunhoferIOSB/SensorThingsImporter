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

import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.TimeInterval;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.TimeValue;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ObservationUploader;
import java.text.ParseException;
import net.time4j.Duration;
import net.time4j.IsoUnit;
import net.time4j.Moment;
import net.time4j.tz.ZonalOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public boolean isValid(Entity obs) throws ImportException {
        if (parsedDuration == null) {
            throw new ImportException("No duration configured.");
        }
        TimeValue phenomenonTime = obs.getProperty(EP_PHENOMENONTIME);
        if (!phenomenonTime.isInterval()) {
            return false;
        }
        TimeInterval interval = phenomenonTime.getInterval();
        Moment start = interval.getStart();
        Moment wantedEnd = parsedDuration.addTo(start.toZonalTimestamp(ZonalOffset.UTC)).atUTC();
        if (interval.getEnd().isSimultaneous(wantedEnd)) {
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
