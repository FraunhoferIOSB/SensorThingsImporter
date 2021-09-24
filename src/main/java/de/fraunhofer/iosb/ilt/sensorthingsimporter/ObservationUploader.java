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
import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.ConfigurationException;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableClass;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.auth.AuthMethod;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.FrostUtils;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Entity;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ext.DataArrayDocument;
import de.fraunhofer.iosb.ilt.sta.model.ext.DataArrayValue;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.net.MalformedURLException;
import java.net.URL;
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
@ConfigurableClass
public class ObservationUploader implements AnnotatedConfigurable<SensorThingsService, Object> {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ObservationUploader.class);

	@ConfigurableField(editor = EditorString.class,
			label = "Service URL", description = "The url of the server to import into.")
	@EditorString.EdOptsString(dflt = "http://localhost:8080/FROST-Server/v1.0")
	private String serviceUrl;

	@ConfigurableField(editor = EditorSubclass.class,
			label = "Auth Method", description = "The authentication method the service uses.",
			optional = true)
	@EditorSubclass.EdOptsSubclass(
			iface = AuthMethod.class)
	private AuthMethod authMethod;

	@ConfigurableField(editor = EditorBoolean.class,
			label = "Use DataArrays",
			description = "Use the SensorThingsAPI DataArray extension to post Observations. "
			+ "This is much more efficient when posting many observations. "
			+ "The number of items grouped together is determined by the messageInterval setting.")
	@EditorBoolean.EdOptsBool()
	private boolean useDataArrays;

	private SensorThingsService service;
	private boolean noAct = false;

	private final Map<Entity, DataArrayValue> davMap = new HashMap<>();

	private Entity lastDatastream;

	private DataArrayValue lastDav;

	private long inserted = 0;
	private long updated = 0;
	private long deleted = 0;

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx, ConfigEditor<?> configEditor) throws ConfigurationException {
		AnnotatedConfigurable.super.configure(config, context, edtCtx, configEditor);
		service = context;

		try {
			service.setEndpoint(new URL(serviceUrl));
			if (authMethod != null) {
				authMethod.setAuth(service);
			}
		} catch (MalformedURLException ex) {
			throw new IllegalArgumentException("Failed to create service.", ex);
		}
	}

	public void setNoAct(boolean noAct) {
		this.noAct = noAct;
	}

	public long getInserted() {
		return inserted;
	}

	public long getUpdated() {
		return updated;
	}

	public long getDeleted() {
		return deleted;
	}

	public void addObservation(Observation obs) throws ServiceFailureException {
		if (obs.getId() != null && !noAct) {
			service.update(obs);
			updated++;
		} else if (!useDataArrays && !noAct) {
			service.create(obs);
			inserted++;
		} else if (useDataArrays) {
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

	public long sendDataArray() throws ServiceFailureException {
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

	public void delete(List<? extends Entity> entities, int threads) throws ServiceFailureException {
		deleted += entities.size();
		new FrostUtils(entities.get(0).getService()).delete(entities, 100);
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
