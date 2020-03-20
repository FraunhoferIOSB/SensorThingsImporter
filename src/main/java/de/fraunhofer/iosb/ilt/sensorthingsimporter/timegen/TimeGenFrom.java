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
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import java.time.Instant;
import java.time.ZonedDateTime;

/**
 *
 * @author scf
 */
public class TimeGenFrom implements TimeGen {

	@ConfigurableField(editor = EditorString.class,
			label = "StartTime", description = "The starting time.")
	@EditorString.EdOptsString(dflt = "2017-01-01T00:00:00Z")
	private String startTime;

	@Override
	public Instant getInstant() {
		return ZonedDateTime.parse(startTime).toInstant();
	}

	@Override
	public Instant getInstant(Datastream ds) {
		return ZonedDateTime.parse(startTime).toInstant();
	}

	@Override
	public Instant getInstant(MultiDatastream mds) {
		return ZonedDateTime.parse(startTime).toInstant();
	}

}
