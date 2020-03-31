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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.ConfigurationException;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.sta.Utils;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 *
 * @author scf
 */
public class ParserTime implements ParserZonedDateTime, AnnotatedConfigurable<Object, Object> {

	@ConfigurableField(editor = EditorString.class,
			label = "Format", description = "The format to use when parsing the time.")
	@EditorString.EdOptsString(dflt = "yyyy-MM-dd HH:mm:ssXXX")
	private String format;

	@ConfigurableField(editor = EditorString.class, optional = true,
			label = "Zone", description = "The timezone to use when the parsed time did not contain a timezone.")
	@EditorString.EdOptsString()
	private String zone;

	DateTimeFormatter formatter;

	@Override
	public void configure(JsonElement config, Object context, Object edtCtx, ConfigEditor<?> configEditor) throws ConfigurationException {
		AnnotatedConfigurable.super.configure(config, context, edtCtx, configEditor);
		init();
	}

	private void init() {
		formatter = DateTimeFormatter.ofPattern(format);
		if (!Utils.isNullOrEmpty(zone)) {
			formatter = formatter.withZone(ZoneId.of(zone));
		}
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
		init();
	}

	public String getZone() {
		return zone;
	}

	public void setZone(String zone) {
		this.zone = zone;
		init();
	}

	@Override
	public ZonedDateTime parse(String time) {
		return ZonedDateTime.from(formatter.parse(time));
	}

	@Override
	public ZonedDateTime parse(JsonNode time) {
		return ZonedDateTime.from(formatter.parse(time.asText()));
	}
}
