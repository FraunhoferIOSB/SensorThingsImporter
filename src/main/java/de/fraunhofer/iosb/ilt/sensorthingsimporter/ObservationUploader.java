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
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
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

	@ConfigurableField(editor = EditorInt.class, optional = true,
			label = "Max Batch", description = "The maximum number of items to send in a batch")
	@EditorInt.EdOptsInt(dflt = 1_000, min = 0, max = Integer.MAX_VALUE, step = 1)
	private int maxBatch;

	private SensorThingsService service;
	private boolean noAct = false;

	private final ThreadLocal<Map<Entity, DataArrayValue>> davMaps = new ThreadLocal<>() {
		@Override
		protected Map<Entity, DataArrayValue> initialValue() {
			return new HashMap<>();
		}
	};
	private final Set<Entity> activeDatastreams = new ConcurrentSkipListSet<>(new EntityComparator());

	private final AtomicLong inserted = new AtomicLong();
	private final AtomicLong updated = new AtomicLong();
	private final AtomicLong deleted = new AtomicLong();
	private final AtomicLong queued = new AtomicLong();

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
		return inserted.get();
	}

	public long getUpdated() {
		return updated.get();
	}

	public long getDeleted() {
		return deleted.get();
	}

	public void addObservation(Observation obs) throws ServiceFailureException {
		if (obs.getId() != null && !noAct) {
			service.update(obs);
			updated.incrementAndGet();
		} else if (!useDataArrays && !noAct) {
			service.create(obs);
			inserted.incrementAndGet();
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
		findDataArrayValue(ds, o)
				.addObservation(o);
		long newqueue = queued.incrementAndGet();
		if (newqueue >= maxBatch) {
			sendDataArray();
		}
	}

	private DataArrayValue findDataArrayValue(Entity ds, Observation o) {
		final Map<Entity, DataArrayValue> davMap = davMaps.get();
		DataArrayValue dav = davMap.get(ds);
		if (dav == null) {
			if (ds instanceof Datastream) {
				dav = new DataArrayValue((Datastream) ds, getDefinedProperties(o));
				activeDatastreams.add(ds);
			} else {
				dav = new DataArrayValue((MultiDatastream) ds, getDefinedProperties(o));
				activeDatastreams.add(ds);
			}
			davMap.put(ds, dav);
		}
		return dav;
	}

	public long sendDataArray() throws ServiceFailureException {
		final Map<Entity, DataArrayValue> davMap = davMaps.get();
		Set<Entity> sentDatastreams = davMap.keySet();
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
			inserted.addAndGet(nonError);
		}
		queued.set(0);
		if (!sentDatastreams.isEmpty() && !activeDatastreams.removeAll(sentDatastreams)) {
			LOGGER.error("Datastream not registered!");
		}
		davMap.clear();
		return inserted.get();
	}

	public void delete(List<? extends Entity> entities, int threads) throws ServiceFailureException {
		deleted.addAndGet(entities.size());
		new FrostUtils(entities.get(0).getService()).delete(entities, 100);
	}

	/**
	 * Check if there are Observations being cached, but not sent, for the given
	 * (Multi)Datastream.
	 *
	 * @param entity The Datastream or MultiDatastream to check for.
	 * @return true if any Thread currently holds Observations for the given
	 * (Multi)Datastream.
	 */
	public boolean isActive(Entity entity) {
		return activeDatastreams.contains(entity);
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

	public static final class EntityComparator implements Comparator<Entity> {

		public EntityComparator() {
		}

		@Override
		public int compare(Entity o1, Entity o2) {
			int ids = o1.getId().compareTo(o2.getId());
			if (ids != 0) {
				return ids;
			}
			return o1.getType().compareTo(o2.getType());
		}
	}
}
