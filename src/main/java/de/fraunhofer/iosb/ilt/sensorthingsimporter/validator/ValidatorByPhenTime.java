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
import de.fraunhofer.iosb.ilt.configurable.editor.EditorNull;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;

/**
 *
 * @author scf
 */
public class ValidatorByPhenTime implements Validator {

	private EditorNull<SensorThingsService, Object> editor = new EditorNull<>("Validator", "Validates the observation against the datastream");

	@Override
	public boolean isValid(Observation obs) throws ImportException {
		try {
			Datastream ds = obs.getDatastream();
			if (ds != null) {
				Observation first = ds.observations().query().select("@iot.id").filter("phenomenonTime eq " + obs.getPhenomenonTime().toString()).first();
				return first == null;
			}
			MultiDatastream mds = obs.getMultiDatastream();
			if (mds != null) {
				Observation first = mds.observations().query().select("@iot.id").filter("phenomenonTime eq " + obs.getPhenomenonTime().toString()).first();
				return first == null;
			}
			throw new ImportException("Observation has no Datastream of Multidatastream set!");
		} catch (ServiceFailureException ex) {
			throw new ImportException("Failed to validate.", ex);
		}
	}

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx) {
	}

	@Override
	public ConfigEditor<SensorThingsService, Object, ?> getConfigEditor(SensorThingsService context, Object edtCtx) {
		return editor;
	}
}
