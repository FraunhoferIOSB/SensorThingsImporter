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

import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Map;

/**
 *
 * @author scf
 */
public class TimeGenFrom implements TimeGen {

	private EditorMap<Map<String, Object>> editor;
	private EditorString editorStartTime;

	@Override
	public Instant getInstant() {
		return ZonedDateTime.parse(editorStartTime.getValue()).toInstant();
	}

	@Override
	public Instant getInstant(Datastream ds) {
		return ZonedDateTime.parse(editorStartTime.getValue()).toInstant();
	}

	@Override
	public Instant getInstant(MultiDatastream mds) {
		return ZonedDateTime.parse(editorStartTime.getValue()).toInstant();
	}

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx) {
		getConfigEditor(context, edtCtx).setConfig(config);
	}

	@Override
	public ConfigEditor<?> getConfigEditor(SensorThingsService context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorMap<>();

			editorStartTime = new EditorString("2017-01-01T00:00:00Z", 1, "StartTime", "The starting time.");
			editor.addOption("startTime", editorStartTime, true);
		}
		return editor;
	}

}
