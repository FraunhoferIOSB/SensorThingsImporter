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
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorNull;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.math.BigDecimal;

/**
 *
 * @author scf
 */
public class ParserBigdecimal implements Parser<BigDecimal> {

	private EditorNull editor = new EditorNull("BigDecimal", "Parses strings into BigDecimals.");

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx, ConfigEditor<?> configEditor) {
		getConfigEditor(context, edtCtx).setConfig(config);
	}

	@Override
	public ConfigEditor<?> getConfigEditor(SensorThingsService context, Object edtCtx) {
		return editor;

	}

	@Override
	public BigDecimal parse(JsonNode value) {
		return new BigDecimal(value.asText());
	}

	@Override
	public BigDecimal parse(String value) {
		return new BigDecimal(value);
	}
}
