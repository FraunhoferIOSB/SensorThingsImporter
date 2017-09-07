/*
 * Copyright (C) 2017 Fraunhofer Institut IOSB, Fraunhoferstr. 1, D 76131
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

import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.math.BigDecimal;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class ValidatorByParameter implements Validator {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ValidatorByParameter.class);
	private EditorMap<Map<String, Object>> editor;
	private EditorBoolean editorUpdate;
	private EditorString editorParamPath;

	private String parameter;
	private boolean update;

	private boolean resultCompare(Object one, Object two) {
		if (one.equals(two)) {
			return true;
		}
		try {
			if (one instanceof BigDecimal) {
				return ((BigDecimal) one).equals(new BigDecimal(two.toString()));
			}
			if (two instanceof BigDecimal) {
				return ((BigDecimal) two).equals(new BigDecimal(one.toString()));
			}
		} catch (NumberFormatException e) {
			LOGGER.trace("Not both bigdecimal.", e);
			// not both bigDecimal.
		}
		return false;
	}

	@Override
	public boolean isValid(Observation obs) throws ImportException {
		Object paramValueRaw = obs.getParameters().get(parameter);
		String paramUrlValue;
		if (paramValueRaw instanceof Number) {
			paramUrlValue = paramValueRaw.toString();
		} else {
			paramUrlValue = "'" + paramValueRaw.toString() + "'";
		}
		try {
			Datastream ds = obs.getDatastream();
			if (ds != null) {
				Observation first = ds.observations().query().select("@iot.id", "Parameters").filter("Parameters/" + parameter + " eq " + paramUrlValue).first();
				if (first == null) {
					return true;
				} else {
					LOGGER.warn("Observation {} with given Parameter {} = {} exists.", first.getId(), parameter, paramUrlValue);
					if (update) {
						obs.setId(first.getId());
						return true;
					}
					return false;
				}
			}
			MultiDatastream mds = obs.getMultiDatastream();
			if (mds != null) {
				Observation first = mds.observations().query().select("@iot.id", "Parameters").filter("Parameters/" + parameter + " eq " + paramUrlValue).first();
				if (first == null) {
					return true;
				} else {
					LOGGER.warn("Observation {} with given Parameter {} = {} exists.", first.getId(), parameter, paramUrlValue);
					if (update) {
						obs.setId(first.getId());
						return true;
					}
					return false;
				}
			}
			throw new ImportException("Observation has no Datastream of Multidatastream set!");
		} catch (ServiceFailureException ex) {
			throw new ImportException("Failed to validate.", ex);
		}
	}

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx) {
		getConfigEditor(context, edtCtx).setConfig(config);
		update = editorUpdate.getValue();
		parameter = editorParamPath.getValue();
	}

	@Override
	public ConfigEditor<?> getConfigEditor(SensorThingsService context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorMap<>();

			editorParamPath = new EditorString("importFileId", 1, "parameter", "The parameter to check.");
			editor.addOption("parameter", editorParamPath, false);

			editorUpdate = new EditorBoolean(false, "Update", "Update existing observations.");
			editor.addOption("update", editorUpdate, true);
		}
		return editor;
	}
}
