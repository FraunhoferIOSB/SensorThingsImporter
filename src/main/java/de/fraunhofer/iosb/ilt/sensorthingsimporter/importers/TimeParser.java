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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.importers;

import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 *
 * @author scf
 */
public class TimeParser implements Parser<ZonedDateTime> {

	private EditorMap<SensorThingsService, Object, Map<String, Object>> editor;
	private EditorString editorTimeFormat;
	private EditorString editorZone;

	DateTimeFormatter formatter;

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx) {
		getConfigEditor(context, edtCtx).setConfig(config, context, edtCtx);
		formatter = DateTimeFormatter.ofPattern(editorTimeFormat.getValue());
		if (!editorZone.getValue().isEmpty()) {
			formatter = formatter.withZone(ZoneId.of(editorZone.getValue()));
		}
	}

	@Override
	public ConfigEditor<SensorThingsService, Object, ?> getConfigEditor(SensorThingsService context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorMap<>();

			editorTimeFormat = new EditorString("yyyy-MM-dd HH:mm:ssX", 1, "Format", "The format to use when parsing the time.");
			editor.addOption("format", editorTimeFormat, false);

			editorZone = new EditorString("", 1, "Zone", "The timezone to use when the parsed time did not contain a timezone.");
			editor.addOption("zone", editorZone, true);
		}
		return editor;

	}

	@Override
	public ZonedDateTime parse(String time) {
		return ZonedDateTime.from(formatter.parse(time));
	}
}
