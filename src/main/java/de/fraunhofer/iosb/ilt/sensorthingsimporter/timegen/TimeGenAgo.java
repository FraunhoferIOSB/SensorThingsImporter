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
import de.fraunhofer.iosb.ilt.configurable.editor.EditorEnum;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

/**
 *
 * @author scf
 */
public class TimeGenAgo implements TimeGen {

	private EditorMap<Map<String, Object>> editor;
	private EditorInt editorAmount;
	private EditorEnum<ChronoUnit> editorUnit;

	@Override
	public Instant getInstant() {
		return Instant.now().minus(editorUnit.getValue().getDuration().multipliedBy(editorAmount.getValue()));
	}

	@Override
	public Instant getInstant(Datastream ds) {
		return Instant.now().minus(editorUnit.getValue().getDuration().multipliedBy(editorAmount.getValue()));
	}

	@Override
	public Instant getInstant(MultiDatastream mds) {
		return Instant.now().minus(editorUnit.getValue().getDuration().multipliedBy(editorAmount.getValue()));
	}

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx) {
		getConfigEditor(context, edtCtx).setConfig(config);
	}

	@Override
	public ConfigEditor<?> getConfigEditor(SensorThingsService context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorMap<>();

			editorUnit = new EditorEnum(ChronoUnit.class, ChronoUnit.DAYS, "Unit", "The unit.");
			editor.addOption("unit", editorUnit, false);

			editorAmount = new EditorInt(0, 999999, 1, 1, "amount", "The amount of steps to go into the past");
			editor.addOption("amount", editorAmount, false);
		}
		return editor;
	}

}
