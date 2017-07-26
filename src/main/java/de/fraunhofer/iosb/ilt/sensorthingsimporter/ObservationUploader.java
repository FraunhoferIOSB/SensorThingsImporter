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
package de.fraunhofer.iosb.ilt.sensorthingsimporter;

import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.Configurable;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.auth.AuthMethod;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Entity;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ext.DataArrayDocument;
import de.fraunhofer.iosb.ilt.sta.model.ext.DataArrayValue;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class ObservationUploader implements Configurable<SensorThingsService, Object> {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ObservationUploader.class);

	private EditorMap<SensorThingsService, Object, Map<String, Object>> editor;
	private EditorString editorService;
	private EditorSubclass<Object, Object, AuthMethod> editorAuthMethod;
	private EditorBoolean editorUseDataArray;

	private SensorThingsService service;
	private boolean noAct = false;
	private boolean dataArray = false;

	private final Map<Entity, DataArrayValue> davMap = new HashMap<>();

	private Entity lastDatastream;

	private DataArrayValue lastDav;

	private int inserted = 0;
	private int updated = 0;

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx) {
		service = context;
		getConfigEditor(service, edtCtx).setConfig(config, service, edtCtx);
		dataArray = editorUseDataArray.getValue();
		try {
			service.setEndpoint(new URI(editorService.getValue()));
			AuthMethod authMethod = editorAuthMethod.getValue();
			if (authMethod != null) {
				authMethod.setAuth(service);
			}
		} catch (URISyntaxException ex) {
			LOGGER.error("Failed to create service.", ex);
			throw new IllegalArgumentException("Failed to create service.", ex);
		}
	}

	@Override
	public ConfigEditor<SensorThingsService, Object, ?> getConfigEditor(SensorThingsService context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorMap<>();
			editorService = new EditorString("https://service.somewhere/path/v1.0", 1, "Service URL", "The url of the server.");
			editor.addOption("serviceUrl", editorService, false);

			editorAuthMethod = new EditorSubclass<>(AuthMethod.class, "Auth Method", "The authentication method the service uses.", false, "className");
			editor.addOption("authMethod", editorAuthMethod, true);

			editorUseDataArray = new EditorBoolean(false, "Use DataArrays",
					"Use the SensorThingsAPI DataArray extension to post Observations. "
					+ "This is much more efficient when posting many observations. "
					+ "The number of items grouped together is determined by the messageInterval setting.");
			editor.addOption("useDataArrays", editorUseDataArray, true);
		}
		return editor;

	}

	public void setNoAct(boolean noAct) {
		this.noAct = noAct;
	}

	public int getInserted() {
		return inserted;
	}

	public int getUpdated() {
		return updated;
	}

	public void addObservation(Observation obs) throws ServiceFailureException {
		if (obs.getId() != null && !noAct) {
			service.update(obs);
			updated++;
		} else if (!dataArray && !noAct) {
			service.create(obs);
			inserted++;
		} else if (dataArray) {
			addToDataArray(obs);
		}
	}

	private void addToDataArray(Observation o) throws ServiceFailureException {
		Entity ds = o.getDatastream();
		if (ds == null) {
			ds = o.getMultiDatastream();
		}
		if (ds == null) {
			throw new IllegalArgumentException("Observation must have a (Multi)Datastream.");
		}
		if (ds != lastDatastream) {
			findDataArrayValue(ds, o);
		}
		lastDav.addObservation(o);
	}

	private void findDataArrayValue(Entity ds, Observation o) {
		DataArrayValue dav = davMap.get(ds);
		if (dav == null) {
			if (ds instanceof Datastream) {
				dav = new DataArrayValue((Datastream) ds, getDefinedProperties(o));
			} else {
				dav = new DataArrayValue((MultiDatastream) ds, getDefinedProperties(o));
			}
			davMap.put(ds, dav);
		}
		lastDav = dav;
		lastDatastream = ds;
	}

	public int sendDataArray() throws ServiceFailureException {
		if (!noAct && !davMap.isEmpty()) {
			DataArrayDocument dad = new DataArrayDocument();
			dad.getValue().addAll(davMap.values());
			List<String> locations = service.create(dad);
			long error = locations.stream().filter(
					location -> location.startsWith("error")
			).count();
			if (error > 0) {
				Optional<String> first = locations.stream().filter(location -> location.startsWith("error")).findFirst();
				LOGGER.warn("Failed to insert {} Observations. First error: {}", error, first);
			}
			long nonError = locations.size() - error;
			inserted += nonError;
		}
		davMap.clear();
		lastDav = null;
		lastDatastream = null;
		return inserted;
	}

	private Set<DataArrayValue.Property> getDefinedProperties(Observation o) {
		Set<DataArrayValue.Property> value = new HashSet<>();
		value.add(DataArrayValue.Property.Result);
		if (o.getPhenomenonTime() != null) {
			value.add(DataArrayValue.Property.PhenomenonTime);
		}
		if (o.getResultTime() != null) {
			value.add(DataArrayValue.Property.ResultTime);
		}
		if (o.getResultQuality() != null) {
			value.add(DataArrayValue.Property.ResultQuality);
		}
		if (o.getParameters() != null) {
			value.add(DataArrayValue.Property.Parameters);
		}
		if (o.getValidTime() != null) {
			value.add(DataArrayValue.Property.ValidTime);
		}
		return value;
	}
}
