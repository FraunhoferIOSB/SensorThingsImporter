/*
 * Copyright (C) 2024 Fraunhofer IOSB
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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.csv;

import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class StringListGeneratorTimeStrings implements StringListGenerator {

	@ConfigurableField(editor = EditorInt.class,
			label = "Now Minus", description = "The number of hours to subtract from now.")
	@EditorInt.EdOptsInt(dflt = 1)
	private int minus = 0;

	@ConfigurableField(editor = EditorInt.class,
			label = "Item Count", description = "The number of items to generate.")
	@EditorInt.EdOptsInt(dflt = 1)
	private int count = 0;

	@ConfigurableField(editor = EditorInt.class,
			label = "Delta", description = "The number if hours to add to the time each step.")
	@EditorInt.EdOptsInt(dflt = 1)
	private int delta = 1;

	@ConfigurableField(editor = EditorString.class,
			label = "Zone", description = "The timezone to convert the time to.")
	@EditorString.EdOptsString(dflt = "Z")
	private String zone = "+01:00";

	@ConfigurableField(editor = EditorString.class,
			label = "Format", description = "The forat to output")
	@EditorString.EdOptsString(dflt = "yyyyMMddHHmmss")
	private String format = "yyyyMMddHHmmss";

	@Override
	public List<String> get() {
		ZoneId zoneId = ZoneId.of(zone);
		ZonedDateTime value = Instant.now()
				.truncatedTo(ChronoUnit.HOURS)
				.minus(minus, ChronoUnit.HOURS)
				.atZone(zoneId);
		List<String> result = new ArrayList<>();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
		for (int i = 0; i < count; i++) {
			result.add(formatter.format(value));
			value = value.plus(delta, ChronoUnit.HOURS);
		}
		return result;
	}

	public int getMinus() {
		return minus;
	}

	public void setMinus(int minus) {
		this.minus = minus;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public int getDelta() {
		return delta;
	}

	public void setDelta(int delta) {
		this.delta = delta;
	}

	public String getZone() {
		return zone;
	}

	public void setZone(String zone) {
		this.zone = zone;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

}
