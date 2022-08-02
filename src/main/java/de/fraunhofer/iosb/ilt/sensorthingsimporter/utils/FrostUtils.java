/*
 * Copyright (C) 2020 Fraunhofer IOSB
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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.Utils;
import de.fraunhofer.iosb.ilt.sta.jackson.ObjectMapperFactory;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Entity;
import de.fraunhofer.iosb.ilt.sta.model.FeatureOfInterest;
import de.fraunhofer.iosb.ilt.sta.model.Location;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import de.fraunhofer.iosb.ilt.sta.model.Sensor;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.TimeObject;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.model.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.sta.query.Query;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.geojson.GeoJsonObject;
import org.geojson.Point;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.referencing.CRS;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

/**
 * @author scf
 */
public final class FrostUtils {

	public static final ZoneId ZONE_ID_Z = ZoneId.of("Z");

	/**
	 * The NULL unit to use for "empty" units.
	 */
	public static final UnitOfMeasurement NULL_UNIT = new UnitOfMeasurement(null, null, null);

	/**
	 * The encoding type for GeoJSON.
	 */
	public static final String ENCODING_GEOJSON = "application/geo+json";

	/**
	 * The content type for GeoJSON.
	 */
	public static final String CONTENT_TYPE_GEOJSON = ENCODING_GEOJSON;

	public static final String OBS_TYPE_MEASUREMENT = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement";

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(FrostUtils.class);

	private final SensorThingsService service;

	private boolean dryRun;

	private int countInsert;
	private int countUpdate;

	public FrostUtils(final SensorThingsService service) {
		this.service = service;
	}

	public void setDryRun(final boolean dryRun) {
		this.dryRun = dryRun;
	}

	public SensorThingsService getService() {
		return service;
	}

	public static String camelCase(final String name) {
		final String[] parts = StringUtils.split(name, '_');
		final StringBuilder result = new StringBuilder(parts[0].toLowerCase());
		for (int idx = 1; idx < parts.length; idx++) {
			final String part = parts[idx];
			result.append(part.substring(0, 1).toUpperCase());
			result.append(part.substring(1).toLowerCase());
		}
		return result.toString();
	}

	public <T extends Entity<T>> void update(final T entity) throws ServiceFailureException {
		if (dryRun) {
			LOGGER.info("Dry Run: Not updating entity {}", entity);
		} else {
			service.update(entity);
			countUpdate++;
		}
	}

	public <T extends Entity<T>> void create(final T entity) throws ServiceFailureException {
		if (dryRun) {
			LOGGER.info("Dry Run: Not creating entity {}", entity);
		} else {
			service.create(entity);
			countInsert++;
		}
	}

	public int getCountInsert() {
		return countInsert;
	}

	public int getCountUpdate() {
		return countUpdate;
	}

	public void resetCounts() {
		countInsert = 0;
		countUpdate = 0;
	}

	public void delete(List<? extends Entity> entities, int threads) throws ServiceFailureException {
		if (threads <= 1) {
			for (Entity entity : entities) {
				service.delete(entity);
			}
			return;
		}
		ExecutorService executor = Executors.newFixedThreadPool(threads);
		List<Future<?>> futures = new ArrayList<>();
		for (Entity entity : entities) {
			futures.add(executor.submit(() -> {
				try {
					service.delete(entity);
				} catch (ServiceFailureException ex) {
					LOGGER.error("Failed to delete {}", entity, ex);
				}
			}));
		}
		for (Future<?> f : futures) {
			try {
				f.get();
			} catch (InterruptedException | ExecutionException ex) {
				LOGGER.error("Maybe Failed to delete?", ex);
			}
		}
		executor.shutdownNow();
	}

	public Thing findOrCreateThing(final String filter, final String name, final String description, final Map<String, Object> properties, final Location location, final Thing cachedThing) throws ServiceFailureException {
		final Thing thing = new Thing(name, description);
		thing.setProperties(properties);
		if (location != null) {
			thing.getLocations().add(location.withOnlyId());
		}
		return findOrCreateThing(filter, thing, cachedThing);
	}

	public Thing findOrCreateThing(final String filter, final Thing newThing, final Thing cachedThing) throws ServiceFailureException {
		Thing thing = null;
		if (cachedThing != null) {
			thing = cachedThing;
		} else {
			final EntityList<Thing> thingList = addOrCreateFilter(service.things().query(), filter, newThing.getName()).expand("Locations($select=id)").list();
			if (thingList.size() > 1) {
				throw new IllegalStateException("More than one thing found with filter " + filter);
			}
			if (thingList.size() == 1) {
				thing = thingList.iterator().next();
			}
		}
		if (thing == null) {
			LOGGER.info("Creating Thing {}.", newThing.getName());
			thing = newThing;
			create(thing);
		} else {
			maybeUpdateThing(newThing, thing);
		}
		return thing;
	}

	public boolean maybeUpdateThing(final Thing newThing, final Thing thingToUpdate) throws ServiceFailureException {
		boolean updated = false;
		boolean updatedLocation = false;
		if (!newThing.getName().equals(thingToUpdate.getName())) {
			updated = true;
			thingToUpdate.setName(newThing.getName());
		}
		if (!newThing.getDescription().equals(thingToUpdate.getDescription())) {
			updated = true;
			thingToUpdate.setDescription(newThing.getDescription());
		}
		if (addProperties(thingToUpdate.getProperties(), newThing.getProperties(), 5)) {
			updated = true;
		}
		if (!newThing.getLocations().isEmpty()) {
			final Location newLocation = newThing.getLocations().toList().get(0);
			final List<Location> locationListToUpdate = thingToUpdate.getLocations().toList();
			if (newLocation.getId() == null) {
				// "new" Location in newThing.
				if (locationListToUpdate.isEmpty()) {
					final Location created = findOrCreateLocation(null, newLocation, null);
					thingToUpdate.getLocations().add(created.withOnlyId());
					updated = true;
					updatedLocation = true;
				} else if (locationListToUpdate.size() == 1) {
					final Location oldLocation = service.locations().find(locationListToUpdate.get(0).getId());
					maybeUpdateLocation(newLocation, oldLocation);
				} else {
					LOGGER.error("Can't check locations for Things with multiple locations if updated Location has no ID.");
				}
			} else {
				if (locationListToUpdate.isEmpty()) {
					thingToUpdate.getLocations().add(newLocation.withOnlyId());
					updated = true;
					updatedLocation = true;
				} else {
					final boolean found = locationListToUpdate.stream().anyMatch(loc -> loc.getId().equals(newLocation.getId()));
					if (!found) {
						thingToUpdate.getLocations().clear();
						thingToUpdate.getLocations().add(newLocation.withOnlyId());
						updated = true;
						updatedLocation = true;
					}
				}
			}
		}
		if (updated) {
			if (!updatedLocation) {
				final List<Location> thingLocations = thingToUpdate.getLocations().toList();
				final List<Location> oldLocations = new ArrayList<>(thingLocations);
				thingLocations.clear();
				update(thingToUpdate);
				thingLocations.addAll(oldLocations);
			} else {
				update(thingToUpdate);
			}
		}
		return updated;
	}

	public Sensor findOrCreateSensor(final String filter, final String name, final String description, final String encodingType, final Object metadata, final Map<String, Object> properties, final Sensor cached) throws ServiceFailureException {
		final Sensor sensor = new Sensor(name, description, encodingType, metadata);
		sensor.setProperties(properties);
		return findOrCreateSensor(filter, sensor, cached);
	}

	public Sensor findOrCreateSensor(final String filter, final Sensor newSensor, final Sensor cachedSensor) throws ServiceFailureException {
		Sensor sensor = null;
		if (cachedSensor != null) {
			sensor = cachedSensor;
		} else {
			final Query<Sensor> query = service.sensors().query();
			final EntityList<Sensor> sensorList = addOrCreateFilter(query, filter, newSensor.getName()).list();
			if (sensorList.size() > 1) {
				throw new IllegalStateException("More than one sensor with name " + newSensor.getName());
			}

			if (sensorList.size() == 1) {
				sensor = sensorList.iterator().next();
			}
		}
		if (sensor == null) {
			LOGGER.info("Creating Sensor {}.", newSensor.getName());
			sensor = newSensor;
			create(sensor);
		} else {
			mayeUpdateSensor(newSensor, sensor);
		}
		return sensor;
	}

	public boolean mayeUpdateSensor(final Sensor newSensor, final Sensor cached) throws ServiceFailureException {
		boolean update = false;
		if (!newSensor.getName().equals(cached.getName())) {
			update = true;
			cached.setName(newSensor.getName());
		}
		if (!newSensor.getDescription().equals(cached.getDescription())) {
			update = true;
			cached.setDescription(newSensor.getDescription());
		}
		if (!newSensor.getEncodingType().equals(cached.getEncodingType())) {
			update = true;
			cached.setEncodingType(newSensor.getEncodingType());
		}
		if (!Objects.equals(newSensor.getMetadata(), cached.getMetadata())) {
			update = true;
			cached.setMetadata(newSensor.getMetadata());
		}
		if (cached.getProperties() == null && newSensor.getProperties() != null && !newSensor.getProperties().isEmpty()) {
			cached.setProperties(newSensor.getProperties());
			update = true;
		} else if (addProperties(cached.getProperties(), newSensor.getProperties(), 5)) {
			update = true;
		}
		if (update) {
			update(cached);
		}
		return update;
	}

	public FeatureOfInterest findOrCreateFeature(final String filter, final String name, final String description, final GeoJsonObject geoJson, final Map<String, Object> properties, final FeatureOfInterest cached) throws ServiceFailureException {
		final FeatureOfInterest foi = new FeatureOfInterest(name, description, CONTENT_TYPE_GEOJSON, geoJson);
		foi.setProperties(properties);
		return findOrCreateFeature(filter, foi, cached);
	}

	public FeatureOfInterest findOrCreateFeature(final String filter, final FeatureOfInterest newFeature, final FeatureOfInterest cachedFeature) throws ServiceFailureException {
		FeatureOfInterest foi = null;
		if (cachedFeature != null) {
			foi = cachedFeature;
		} else {
			final Query<FeatureOfInterest> query = service.featuresOfInterest().query();
			final EntityList<FeatureOfInterest> foiList = addOrCreateFilter(query, filter, newFeature.getName()).list();
			if (foiList.size() > 1) {
				throw new IllegalStateException("More than one FeatureOfInterest with name " + newFeature.getName());
			}
			if (foiList.size() == 1) {
				foi = foiList.iterator().next();
			}
		}
		if (foi == null) {
			LOGGER.info("Creating Feature {}.", newFeature.getName());
			foi = newFeature;
			create(foi);
		} else {
			maybeUpdateFeatureOfInterest(newFeature, foi);
		}
		return foi;
	}

	public boolean maybeUpdateFeatureOfInterest(final FeatureOfInterest newFeature, final FeatureOfInterest foiToUpdate) throws ServiceFailureException {
		boolean update = false;
		if (!newFeature.getName().equals(foiToUpdate.getName())) {
			update = true;
			foiToUpdate.setName(newFeature.getName());
		}
		if (!newFeature.getDescription().equals(foiToUpdate.getDescription())) {
			update = true;
			foiToUpdate.setDescription(newFeature.getDescription());
		}
		final ObjectMapper objectMapper = ObjectMapperFactory.get();
		try {
			if (!objectMapper.writeValueAsString(newFeature.getFeature()).equals(objectMapper.writeValueAsString(foiToUpdate.getFeature()))) {
				update = true;
				LOGGER.debug("Location changed from {} to {}", foiToUpdate.getFeature(), newFeature.getFeature());
				foiToUpdate.setFeature(newFeature.getFeature());
			}
		} catch (final JsonProcessingException exc) {
			LOGGER.error("Failed to compare geoJson objects.", exc);
		}

		if (foiToUpdate.getProperties() == null && newFeature.getProperties() != null) {
			foiToUpdate.setProperties(newFeature.getProperties());
			update = true;
		}
		if (addProperties(foiToUpdate.getProperties(), newFeature.getProperties(), 5)) {
			update = true;
		}
		if (update) {
			update(foiToUpdate);
		}
		return update;
	}

	public ObservedProperty findOrCreateOp(final String filter, final String name, final String def, final String description, final Map<String, Object> properties, final ObservedProperty cached) throws ServiceFailureException {
		final ObservedProperty observedProperty = new ObservedProperty(name, def, description);
		observedProperty.setProperties(properties);
		return findOrCreateOp(filter, observedProperty, cached);
	}

	public ObservedProperty findOrCreateOp(final String filter, final ObservedProperty newObsProp, final ObservedProperty cachedObsProp) throws ServiceFailureException {
		ObservedProperty observedProperty = null;
		if (cachedObsProp != null) {
			observedProperty = cachedObsProp;
		} else {
			final Query<ObservedProperty> query = service.observedProperties().query();
			final EntityList<ObservedProperty> opList = addOrCreateFilter(query, filter, newObsProp.getName()).list();
			if (opList.size() > 1) {
				throw new IllegalStateException("More than one observedProperty with name " + newObsProp.getName());
			}
			if (opList.size() == 1) {
				observedProperty = opList.iterator().next();
			}
		}
		if (observedProperty == null) {
			LOGGER.info("Creating ObservedProperty {}.", newObsProp.getName());
			observedProperty = newObsProp;
			create(observedProperty);
		} else {
			maybeUpdateOp(newObsProp, observedProperty);
		}
		return observedProperty;
	}

	public boolean maybeUpdateOp(final ObservedProperty newObsProp, final ObservedProperty opToUpdate) throws ServiceFailureException {
		boolean update = false;
		if (!newObsProp.getName().equals(opToUpdate.getName())) {
			update = true;
			opToUpdate.setName(newObsProp.getName());
		}
		if (!newObsProp.getDescription().equals(opToUpdate.getDescription())) {
			update = true;
			opToUpdate.setDescription(newObsProp.getDescription());
		}
		if (opToUpdate.getProperties() == null && newObsProp.getProperties() != null && !newObsProp.getProperties().isEmpty()) {
			opToUpdate.setProperties(newObsProp.getProperties());
			update = true;
		}
		if (addProperties(opToUpdate.getProperties(), newObsProp.getProperties(), 5)) {
			update = true;
		}
		if (update) {
			update(opToUpdate);
		}
		return update;
	}

	public Datastream findOrCreateDatastream(
			final String filter,
			final String name,
			final String desc,
			final Map<String, Object> properties,
			final UnitOfMeasurement uom,
			final Thing t,
			final ObservedProperty op,
			final Sensor s,
			final Datastream cached) throws ServiceFailureException {
		Datastream ds = new Datastream(name, desc, OBS_TYPE_MEASUREMENT, uom);
		ds.setProperties(properties);
		ds.setThing(t);
		ds.setSensor(s);
		ds.setObservedProperty(op);
		return findOrCreateDatastream(filter, ds, cached);
	}

	public Datastream findOrCreateDatastream(final String filter, final Datastream newDatastream, final Datastream cached) throws ServiceFailureException {
		Datastream datastream = null;
		if (cached != null) {
			datastream = cached;
		} else {
			final Query<Datastream> query = newDatastream.getThing().datastreams().query();
			final EntityList<Datastream> datastreamList = addOrCreateFilter(query, filter, newDatastream.getName()).list();
			if (datastreamList.size() > 1) {
				throw new IllegalStateException("More than one datastream matches filter " + filter);
			}
			if (datastreamList.size() == 1) {
				datastream = datastreamList.iterator().next();
			}
		}
		if (datastream == null) {
			LOGGER.info("Creating Datastream {}.", newDatastream.getName());
			datastream = newDatastream;
			create(datastream);
		} else {
			maybeUpdateDatastream(newDatastream, datastream);
		}
		return datastream;
	}

	public boolean maybeUpdateDatastream(final Datastream newDatastream, final Datastream dsToUpdate) throws ServiceFailureException {
		boolean update = false;
		if (!newDatastream.getName().equals(dsToUpdate.getName())) {
			dsToUpdate.setName(newDatastream.getName());
			update = true;
		}
		if (!newDatastream.getDescription().equals(dsToUpdate.getDescription())) {
			dsToUpdate.setDescription(newDatastream.getDescription());
			update = true;
		}
		if (dsToUpdate.getProperties() == null && newDatastream.getProperties() != null && !newDatastream.getProperties().isEmpty()) {
			dsToUpdate.setProperties(newDatastream.getProperties());
			update = true;
		}
		if (addProperties(dsToUpdate.getProperties(), newDatastream.getProperties(), 5)) {
			update = true;
		}
		if (!newDatastream.getUnitOfMeasurement().equals(dsToUpdate.getUnitOfMeasurement())) {
			dsToUpdate.setUnitOfMeasurement(newDatastream.getUnitOfMeasurement());
			update = true;
		}
		if (!dsToUpdate.getObservedProperty().getId().equals(newDatastream.getObservedProperty().getId())) {
			dsToUpdate.setObservedProperty(newDatastream.getObservedProperty().withOnlyId());
			update = true;
		}
		if (update) {
			update(dsToUpdate);
		}
		return update;
	}

	public MultiDatastream findOrCreateMultiDatastream(final String filter, final String name, final String desc, final List<UnitOfMeasurement> uoms, final Thing thing, final List<ObservedProperty> observedProperties, final Sensor sensor, final Map<String, Object> props, final MultiDatastream cached) throws ServiceFailureException {
		MultiDatastream mds = null;
		if (cached != null) {
			mds = cached;
		} else {
			final Query<MultiDatastream> query = service.multiDatastreams().query();
			final EntityList<MultiDatastream> mdsList = addOrCreateFilter(query, filter, name).list();
			if (mdsList.size() > 1) {
				throw new IllegalStateException("More than one multidatastream with name " + name);
			}

			if (mdsList.size() == 1) {
				mds = mdsList.iterator().next();
			}
		}
		if (mds == null) {
			LOGGER.info("Creating multiDatastream {}.", name);
			final List<String> dataTypes = observedProperties.stream().map(observedProperty -> OBS_TYPE_MEASUREMENT).collect(Collectors.toList());
			mds = new MultiDatastream(name, desc, dataTypes, uoms);
			mds.setProperties(props);
			mds.setThing(thing);
			mds.setSensor(sensor);
			mds.getObservedProperties().addAll(observedProperties);
			create(mds);
		} else {
			maybeUpdateMultiDatastream(name, desc, props, mds);
		}
		return mds;
	}

	public boolean maybeUpdateMultiDatastream(final String name, final String desc, final Map<String, Object> props, final MultiDatastream mdsToUpdate) throws ServiceFailureException {
		boolean update = false;
		if (!name.equals(mdsToUpdate.getName())) {
			update = true;
			mdsToUpdate.setName(name);
		}
		if (!desc.equals(mdsToUpdate.getDescription())) {
			update = true;
			mdsToUpdate.setDescription(desc);
		}
		if (mdsToUpdate.getProperties() == null && props != null) {
			mdsToUpdate.setProperties(props);
			update = true;
		}
		if (addProperties(mdsToUpdate.getProperties(), props, 5)) {
			update = true;
		}
		if (update) {
			update(mdsToUpdate);
		}
		return update;
	}

	public Location findOrCreateLocation(final String name, final String description, final Map<String, Object> properties, final GeoJsonObject geoJson) throws ServiceFailureException {
		final String filter = "name eq '" + Utils.escapeForStringConstant(name) + "'";
		return findOrCreateLocation(filter, name, description, properties, geoJson, null);
	}

	public Location findOrCreateLocation(final String filter, final String name, final String description, final Map<String, Object> properties, final GeoJsonObject geoJson, final Location cached) throws ServiceFailureException {
		final Location location = new Location(name, description, ENCODING_GEOJSON, geoJson);
		location.setProperties(properties);
		return findOrCreateLocation(filter, location, cached);
	}

	public Location findOrCreateLocation(final String filter, final Location newLocation, final Location cached) throws ServiceFailureException {
		Location location = null;
		if (cached != null) {
			location = cached;
		} else {
			final EntityList<Location> lList = service.locations().query().filter(filter).list();
			if (lList.size() > 1) {
				throw new IllegalStateException("More than one Location matches filter: " + filter);
			}
			if (lList.size() == 1) {
				location = lList.iterator().next();
			}
		}
		if (location == null) {
			LOGGER.info("Creating Location {}.", newLocation.getName());
			location = newLocation;
			create(location);
		} else {
			maybeUpdateLocation(newLocation, location);
		}
		return location;
	}

	public boolean maybeUpdateLocation(final Location newLocation, final Location locationToUpdate) throws ServiceFailureException {
		boolean updated = false;
		if (!locationToUpdate.getName().equals(newLocation.getName())) {
			updated = true;
			locationToUpdate.setName(newLocation.getName());
		}
		if (!locationToUpdate.getDescription().equals(newLocation.getDescription())) {
			updated = true;
			locationToUpdate.setDescription(newLocation.getDescription());
		}
		if (addProperties(locationToUpdate.getProperties(), newLocation.getProperties(), 10)) {
			updated = true;
		}
		final ObjectMapper objectMapper = ObjectMapperFactory.get();
		try {
			if (!objectMapper.writeValueAsString(newLocation.getLocation()).equals(objectMapper.writeValueAsString(locationToUpdate.getLocation()))) {
				updated = true;
				LOGGER.debug("Location changed from {} to {}", locationToUpdate.getLocation(), newLocation.getLocation());
				locationToUpdate.setLocation(newLocation.getLocation());
			}
		} catch (final JsonProcessingException exc) {
			LOGGER.error("Failed to compare geoJson objects.", exc);
		}
		if (updated) {
			update(locationToUpdate);
		}
		return updated;
	}

	public static String quoteForUrl(final Object in) {
		if (in instanceof Number) {
			return in.toString();
		}
		return "'" + Utils.escapeForStringConstant(String.valueOf(in)) + "'";
	}

	public static <Q extends Entity<Q>> Query<Q> addOrCreateFilter(final Query<Q> query, final String filter, final String name) {
		if (Utils.isNullOrEmpty(filter)) {
			return query.filter("name eq '" + Utils.escapeForStringConstant(name) + "'");
		}
		return query.filter(filter);
	}

	public static Instant phenTimeToInstant(final TimeObject phenTime) {
		if (phenTime.isInterval()) {
			final Interval interval = phenTime.getAsInterval();
			return interval.getStart().plus(interval.toDuration().dividedBy(2));
		}
		return phenTime.getAsDateTime().toInstant();
	}

	/**
	 * Checks if all entries in source exist in target, with the same value.If
	 * not, target is updated and true is returned. Sub-maps are recursed.
	 *
	 * @param target the target map to update
	 * @param source the source map to get values from
	 * @param maxDepth The maximum depth to recurse.
	 * @return true if target was updated, false if not.
	 */
	public static boolean addProperties(final Map<String, Object> target, final Map<String, Object> source, final int maxDepth) {
		if (target == null) {
			return false;
		}

		boolean updated = false;
		for (final Map.Entry<String, Object> entry : source.entrySet()) {
			final String key = entry.getKey();
			final Object value = entry.getValue();
			if ((value == null || String.valueOf(value).isEmpty()) && !target.containsKey(key)) {
				continue;
			}
			if (!target.containsKey(key)) {
				target.put(key, value);
				updated = true;
			} else {
				final Object tValue = target.get(key);
				if (value instanceof Map) {
					if (maxDepth > 0) {
						final Map valueMap = (Map) value;
						if (tValue instanceof Map) {
							final Map tValueMap = (Map) tValue;
							updated = updated || addProperties(tValueMap, valueMap, maxDepth - 1);
						} else {
							target.put(key, value);
							updated = true;
						}
					}
				} else if (!resultCompare(value, tValue)) {
					target.put(key, value);
					updated = true;
				}

			}
		}
		return updated;
	}

	private static boolean resultCompare(final Object one, final Object two) {
		if (one == null) {
			return two == null;
		}
		if (two == null) {
			return false;
		}
		if (one.equals(two)) {
			return true;
		}

		try {
			if (one instanceof Long && two instanceof Integer) {
				return ((Long) one).equals(Long.valueOf((Integer) two));
			}
			if (two instanceof Long && one instanceof Integer) {
				return ((Long) two).equals(Long.valueOf((Integer) one));
			}
			if (one instanceof BigDecimal) {
				return ((BigDecimal) one).compareTo(new BigDecimal(two.toString())) == 0;
			}
			if (two instanceof BigDecimal) {
				return ((BigDecimal) two).compareTo(new BigDecimal(one.toString())) == 0;
			}
			if (one instanceof BigInteger) {
				return ((BigInteger) one).equals(new BigInteger(two.toString()));
			}
			if (two instanceof BigInteger) {
				return ((BigInteger) two).equals(new BigInteger(one.toString()));
			}
			if (one instanceof Instant) {
				return ((Instant) one).equals(Instant.parse(two.toString()));
			}
			if (two instanceof Instant) {
				return ((Instant) two).equals(Instant.parse(one.toString()));
			}
			if (one instanceof Collection && two instanceof Collection) {
				final Collection cOne = (Collection) one;
				final Collection cTwo = (Collection) two;
				final Iterator iTwo = cTwo.iterator();
				for (final Object itemOne : cOne) {
					if (!iTwo.hasNext() || !resultCompare(itemOne, iTwo.next())) {
						// Collection one is longer than two
						return false;
					}
				}
				if (iTwo.hasNext()) {
					// Collection two is longer than one.
					return false;
				}
				return true;
			}
		} catch (final NumberFormatException exc) {
			LOGGER.trace("Not both bigdecimal.", exc);
			// not both bigDecimal.
		}
		return false;
	}

	public static Instant instantFrom(TimeObject time) {
		return time.isInterval() ? time.getAsInterval().getStart() : time.getAsDateTime().toInstant();
	}

	/**
	 * Creates an Instant from a timestamp. If the timestamp has no timezone
	 * information, then the given timeZone is used.
	 *
	 * @param timestamp
	 * @param timeZone
	 * @return
	 */
	public static Instant timestampToInstant(final Timestamp timestamp, final ZoneId timeZone) {
		try {
			return timestamp.toInstant();
		} catch (final Exception exc) {
			LOGGER.trace("Timestamp without timezone?", exc);
		}
		return ZonedDateTime.of(timestamp.toLocalDateTime(), timeZone).toInstant();

	}

	public static TimeObject timeObjectFrom(final Timestamp timestamp, final ZoneId timeZone) {
		try {
			final Instant instant = timestamp.toInstant();
			return new TimeObject(ZonedDateTime.from(instant));
		} catch (final Exception exc) {
			LOGGER.trace("Timestamp without timezone?", exc);
			return new TimeObject(ZonedDateTime.of(timestamp.toLocalDateTime(), timeZone));
		}
	}

	public static TimeObject timeObjectFrom(final Date date) {
		final Instant instant = date.toInstant();
		return new TimeObject(ZonedDateTime.from(instant));
	}

	/**
	 * Creates a timeObject from timestamps. If the timestamps have no timezone
	 * information, then the given timeZone is used.
	 *
	 * @param start the starting timeStamp
	 * @param end the ending timeStamp
	 * @param timeZone the time zone to cast the times to.
	 * @return a timeobject.
	 */
	public static TimeObject timeObjectFrom(final Timestamp start, final Timestamp end, final ZoneId timeZone) {
		final Instant instantStart = timestampToInstant(start, timeZone);
		final Instant instantEnd = timestampToInstant(end, timeZone);
		final Interval interval = Interval.of(instantStart, instantEnd);
		return new TimeObject(interval);
	}

	/**
	 * Creates a timeObject from timestamps. If the timestamps have no timezone
	 * information, then the given timeZone is used.
	 *
	 * @param start the starting timeStamp
	 * @param end the ending timeStamp
	 * @return a timeobject.
	 */
	public static TimeObject timeObjectFrom(final Date start, final Date end) {

		final Instant instantStart = start.toInstant();
		final Instant instantEnd = end.toInstant();
		final Interval interval = Interval.of(instantStart, instantEnd);
		return new TimeObject(interval);
	}

	/**
	 * Creates a timeObject from ISO timestamps.
	 *
	 * @param start the starting timeStamp
	 * @param end the ending timeStamp
	 * @return a timeobject.
	 */
	public static TimeObject timeObjectFrom(final String start, final String end) {
		final Instant instantStart = ZonedDateTime.parse(start).toInstant();
		final Instant instantEnd = ZonedDateTime.parse(end).toInstant();
		final Interval interval = Interval.of(instantStart, instantEnd);
		return new TimeObject(interval);
	}

	public static Point convertCoordinates(final double first, final double second, final String crsName, int numberScale) {
		if (Utils.isNullOrEmpty(crsName)) {
			return new Point(
					new BigDecimal(second).setScale(numberScale, RoundingMode.HALF_EVEN).doubleValue(),
					new BigDecimal(first).setScale(numberScale, RoundingMode.HALF_EVEN).doubleValue());
		}
		try {
			String fullCrs = crsName;
			if (!fullCrs.contains(":")) {
				fullCrs = "EPSG:" + fullCrs;
			}
			final CoordinateReferenceSystem sourceCrs = CRS.decode(fullCrs);
			final CoordinateReferenceSystem targetCrs = CRS.decode("EPSG:4326");
			final MathTransform transform = CRS.findMathTransform(sourceCrs, targetCrs);
			final DirectPosition2D sourcePoint = new DirectPosition2D(sourceCrs, first, second);
			final DirectPosition2D targetPoint = new DirectPosition2D(targetCrs);
			transform.transform(sourcePoint, targetPoint);
			return new Point(
					new BigDecimal(targetPoint.y).setScale(numberScale, RoundingMode.HALF_EVEN).doubleValue(),
					new BigDecimal(targetPoint.x).setScale(numberScale, RoundingMode.HALF_EVEN).doubleValue());
		} catch (FactoryException | MismatchedDimensionException | org.opengis.referencing.operation.TransformException exc) {
			throw new RuntimeException("Failed to convert coordinates", exc);
		}
	}

	public static Point convertCoordinates(final String locationPos, final String locationSrsName, int numberScale, boolean flip) {
		final String[] coordinates = locationPos.split(" ");
		if (flip) {
			return convertCoordinates(Double.parseDouble(coordinates[1]), Double.parseDouble(coordinates[0]), locationSrsName, numberScale);
		} else {
			return convertCoordinates(Double.parseDouble(coordinates[0]), Double.parseDouble(coordinates[1]), locationSrsName, numberScale);
		}
	}

	public static Map<String, Object> putIntoSubMap(final Map<String, Object> map, final String subMapName, final String key, final Object value) {
		final Map<String, Object> subMap = (Map<String, Object>) map.computeIfAbsent(subMapName, (final String t) -> new HashMap<>());
		subMap.put(key, value);
		return subMap;
	}

	public static String afterLastSlash(final String input) {
		return input.substring(input.lastIndexOf('/') + 1);
	}

	public static PropertyBuilder propertiesBuilder() {
		return new PropertyBuilder();
	}

	public static class PropertyBuilder {

		Map<String, Object> properties = new HashMap<>();

		public PropertyBuilder addItem(final String key, final Object value) {
			properties.put(key, value);
			return this;
		}

		public PropertyBuilder addPath(final String path, final Object value) {
			CollectionsHelper.setOn(properties, path, value);
			return this;
		}

		public Map<String, Object> build() {
			return properties;
		}
	}

}
