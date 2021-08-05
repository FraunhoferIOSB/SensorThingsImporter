/*
 * Copyright (C) 2017 Fraunhofer IOSB
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

import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.TimeObject;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

/**
 *
 * @author scf
 */
public class TimeGenNewer implements TimeGen {

	/**
	 * The logger for this class.
	 */
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TimeGenNewer.class);

	@ConfigurableField(editor = EditorString.class,
			label = "StartTime", description = "The starting time, if the datastream has no observations yet.")
	@EditorString.EdOptsString(dflt = "2017-01-01T00:00:00Z")
	private String startTime;

	@Override
	public Instant getInstant() {
		return ZonedDateTime.parse(startTime).toInstant();
	}

	@Override
	public Instant getInstant(Datastream ds) {
		if (ds.getPhenomenonTime() != null) {
			Interval phenomenonTime = ds.getPhenomenonTime();
			return phenomenonTime.getEnd().plusSeconds(1);
		}
		if (!ds.getObservations().toList().isEmpty()) {
			TimeObject phenomenonTime = ds.getObservations().toList().get(0).getPhenomenonTime();
			if (phenomenonTime.isInterval()) {
				return phenomenonTime.getAsInterval().getEnd();
			}
			return phenomenonTime.getAsDateTime().plusSeconds(1).toInstant();
		}
		try {
			List<Observation> obsList = ds.observations().query().top(1).orderBy("phenomenonTime desc").list().toList();
			if (!obsList.isEmpty()) {
				TimeObject phenomenonTime = obsList.get(0).getPhenomenonTime();
				if (phenomenonTime.isInterval()) {
					return phenomenonTime.getAsInterval().getEnd();
				}
				return phenomenonTime.getAsDateTime().plusSeconds(1).toInstant();
			}
		} catch (ServiceFailureException ex) {
			LOGGER.error("Failed to fetch last Observation.", ex);
		}
		return ZonedDateTime.parse(startTime).toInstant();
	}

	@Override
	public Instant getInstant(MultiDatastream mds) {
		if (mds.getPhenomenonTime() != null) {
			Interval phenomenonTime = mds.getPhenomenonTime();
			return phenomenonTime.getEnd();
		}
		if (mds.getObservations().toList().isEmpty()) {
			return ZonedDateTime.parse(startTime).toInstant();
		} else {
			return mds.getObservations().toList().get(0).getPhenomenonTime().getAsDateTime().plusSeconds(1).toInstant();
		}
	}

}
