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
import de.fraunhofer.iosb.ilt.configurable.AbstractConfigurable;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class ValidatorByParameter extends AbstractConfigurable<SensorThingsService, Object> implements Validator {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ValidatorByParameter.class);

	@ConfigurableField(
			label = "parameters",
			description = "The parameters to check. Comma separated, no spaces.",
			editor = EditorString.class)
	@EditorString.EdOptsString(dflt = "importFileId,importFileBase")
	private String parameter;

	@ConfigurableField(
			label = "Check Time",
			description = "Check the phenomenonTime too",
			editor = EditorBoolean.class)
	@EditorBoolean.EdOptsBool()
	private boolean checkPhenomenonTime;

	@ConfigurableField(
			label = "Update",
			description = "Update existing observations.",
			editor = EditorBoolean.class)
	@EditorBoolean.EdOptsBool()
	private boolean update;

	private List<String> parameters;

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx) {
		super.configure(config, context, edtCtx);
		String[] split = parameter.split(",");
		parameters = Arrays.asList(split);
	}

	private String buildFilter(Observation obs) {
		StringBuilder filter = new StringBuilder();
		boolean first = true;
		if (checkPhenomenonTime) {
			filter.append("phenomenonTime eq ");
			filter.append(obs.getPhenomenonTime().toString());
			first = false;
		}
		for (String param : parameters) {
			if (first) {
				first = false;
			} else {
				filter.append(" and ");
			}
			Object paramValueRaw = obs.getParameters().get(param);
			String paramUrlValue;
			if (paramValueRaw instanceof Number) {
				paramUrlValue = paramValueRaw.toString();
			} else {
				paramUrlValue = "'" + paramValueRaw.toString() + "'";
			}
			filter.append("Parameters/").append(param).append(" eq ").append(paramUrlValue);
		}
		return filter.toString();
	}

	@Override
	public boolean isValid(Observation obs) throws ImportException {
		String filter = buildFilter(obs);
		try {
			Datastream ds = obs.getDatastream();
			if (ds != null) {
				Observation first = ds.observations()
						.query()
						.select("@iot.id", "Parameters")
						.filter(filter)
						.first();
				if (first == null) {
					return true;
				} else {
					LOGGER.trace("Observation {} with given Parameters {} = {} exists.", first.getId(), parameters, obs.getParameters());
					if (update) {
						obs.setId(first.getId());
						return true;
					}
					return false;
				}
			}
			MultiDatastream mds = obs.getMultiDatastream();
			if (mds != null) {
				Observation first = mds.observations()
						.query()
						.select("@iot.id", "Parameters")
						.filter(filter)
						.first();
				if (first == null) {
					return true;
				} else {
					LOGGER.trace("Observation {} with given Parameter {} = {} exists.", first.getId(), parameters, obs.getParameters());
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

	/**
	 * @return the parameter
	 */
	public String getParameter() {
		return parameter;
	}

	/**
	 * @param parameter the parameter to set
	 */
	public void setParameter(String parameter) {
		this.parameter = parameter;
	}

	/**
	 * @return the update
	 */
	public boolean isUpdate() {
		return update;
	}

	/**
	 * @param update the update to set
	 */
	public void setUpdate(boolean update) {
		this.update = update;
	}

}
