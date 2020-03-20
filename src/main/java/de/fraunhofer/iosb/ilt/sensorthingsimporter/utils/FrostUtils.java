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
import com.google.common.base.Strings;
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
import org.geojson.GeoJsonObject;
import org.geotools.geometry.DirectPosition2D;
import org.geotools.referencing.CRS;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

/**
 * @author scf
 */
public final class FrostUtils {

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

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(FrostUtils.class);

	private final SensorThingsService service;
	private boolean dryRun = false;

	public FrostUtils(SensorThingsService service) {
		this.service = service;
	}

	public void setDryRun(boolean dryRun) {
		this.dryRun = dryRun;
	}

	public <T extends Entity<T>> void update(T entity) throws ServiceFailureException {
		if (dryRun) {
			LOGGER.info("Dry Run: Not updating entity " + entity);
		} else {
			service.update(entity);
		}
	}

	public <T extends Entity<T>> void create(T entity) throws ServiceFailureException {
		if (dryRun) {
			LOGGER.info("Dry Run: Not creating entity " + entity);
		} else {
			service.create(entity);
		}
	}

	public Thing findOrCreateThing(
			final String filter,
			final String name,
			final String description,
			final Map<String, Object> properties,
			final Location location,
			final Thing cachedThing) throws ServiceFailureException {
		Thing thing = null;
		if (cachedThing != null) {
			thing = cachedThing;
		} else {
			EntityList<Thing> thingList;
			thingList = addOrCreateFilter(service.things().query(), filter, name).expand("Locations($select=id)").list();
			if (thingList.size() > 1) {
				throw new IllegalStateException("More than one thing found with filter " + filter);
			}
			if (thingList.size() == 1) {
				thing = thingList.iterator().next();
			}
		}
		if (thing == null) {
			LOGGER.info("Creating Thing {}.", name);
			thing = new Thing(name, description);
			thing.setProperties(properties);
			if (location != null) {
				thing.getLocations().add(location.withOnlyId());
			}
			create(thing);
		} else {
			maybeUpdateThing(name, description, properties, location, thing);
		}
		return thing;
	}

	public boolean maybeUpdateThing(
			final String name,
			final String description,
			final Map<String, Object> properties,
			final Location location,
			final Thing cached) throws ServiceFailureException {
		boolean updated = false;
		if (!name.equals(cached.getName())) {
			updated = true;
			cached.setName(name);
		}
		if (!description.equals(cached.getDescription())) {
			updated = true;
			cached.setDescription(description);
		}
		if (addProperties(cached.getProperties(), properties, 5)) {
			updated = true;
		}
		if (location != null) {
			final List<Location> locationList = cached.getLocations().toList();
			if (locationList.isEmpty()) {
				cached.getLocations().add(location.withOnlyId());
				updated = true;
			} else {
				boolean found = false;
				for (Location loc : locationList) {
					if (loc.getId().equals(location.getId())) {
						found = true;
						break;
					}
				}
				if (!found) {
					cached.getLocations().clear();
					cached.getLocations().add(location.withOnlyId());
					updated = true;
				}
			}
		}
		if (updated) {
			update(cached);
		}
		return updated;
	}

	public Sensor findOrCreateSensor(
			final String filter,
			final String name,
			final String description,
			final String encodingType,
			final Object metadata,
			final Map<String, Object> properties,
			final Sensor cached) throws ServiceFailureException {
		Sensor sensor = null;
		if (cached != null) {
			sensor = cached;
		} else {
			final Query<Sensor> query = service.sensors().query();
			final EntityList<Sensor> sensorList = addOrCreateFilter(query, filter, name).list();
			if (sensorList.size() > 1) {
				throw new IllegalStateException("More than one sensor with name " + name);
			}

			if (sensorList.size() == 1) {
				sensor = sensorList.iterator().next();
			}
		}
		if (sensor == null) {
			LOGGER.info("Creating Sensor {}.", name);
			sensor = new Sensor(name, description, encodingType, metadata);
			sensor.setProperties(properties);
			create(sensor);
		} else {
			mayeUpdateSensor(name, description, encodingType, metadata, properties, sensor);
		}
		return sensor;
	}

	public boolean mayeUpdateSensor(
			final String name,
			final String description,
			final String encodingType,
			final Object metadata,
			final Map<String, Object> properties,
			final Sensor cached) throws ServiceFailureException {
		boolean update = false;
		if (!name.equals(cached.getName())) {
			update = true;
			cached.setName(name);
		}
		if (!description.equals(cached.getDescription())) {
			update = true;
			cached.setDescription(description);
		}
		if (!encodingType.equals(cached.getEncodingType())) {
			update = true;
			cached.setEncodingType(encodingType);
		}
		if (!Objects.equals(metadata, cached.getMetadata())) {
			update = true;
			cached.setMetadata(metadata);
		}
		if (cached.getProperties() == null && properties != null) {
			cached.setProperties(properties);
			update = true;
		} else if (addProperties(cached.getProperties(), properties, 5)) {
			update = true;
		}
		if (update) {
			update(cached);
		}
		return update;
	}

	public FeatureOfInterest findOrCreateFeature(
			final String filter,
			final String name,
			final String description,
			final GeoJsonObject geoJson,
			final Map<String, Object> properties,
			final FeatureOfInterest cached) throws ServiceFailureException {
		FeatureOfInterest foi = null;
		if (cached != null) {
			foi = cached;
		} else {
			final Query<FeatureOfInterest> query = service.featuresOfInterest().query();
			final EntityList<FeatureOfInterest> foiList = addOrCreateFilter(query, filter, name).list();
			if (foiList.size() > 1) {
				throw new IllegalStateException("More than one FeatureOfInterest with name " + name);
			}
			if (foiList.size() == 1) {
				foi = foiList.iterator().next();
			}
		}
		if (foi == null) {
			LOGGER.info("Creating Feature {}.", name);
			foi = new FeatureOfInterest(name, description, CONTENT_TYPE_GEOJSON, geoJson);
			foi.setProperties(properties);
			create(foi);
		} else {
			maybeUpdateFeatureOfInterest(name, description, geoJson, properties, foi);
		}
		return foi;
	}

	public boolean maybeUpdateFeatureOfInterest(
			final String name,
			final String description,
			final GeoJsonObject geoJson,
			final Map<String, Object> properties,
			final FeatureOfInterest cached) throws ServiceFailureException {
		boolean update = false;
		if (!name.equals(cached.getName())) {
			update = true;
			cached.setName(name);
		}
		if (!description.equals(cached.getDescription())) {
			update = true;
			cached.setDescription(description);
		}
		ObjectMapper om = ObjectMapperFactory.get();
		try {
			if (!om.writeValueAsString(geoJson).equals(om.writeValueAsString(cached.getFeature()))) {
				update = true;
				LOGGER.debug("Location changed from {} to {}", cached.getFeature(), geoJson);
				cached.setFeature(geoJson);
			}
		} catch (JsonProcessingException ex) {
			LOGGER.error("Failed to compare geoJson objects.");
		}

		if (cached.getProperties() == null && properties != null) {
			cached.setProperties(properties);
			update = true;
		}
		if (addProperties(cached.getProperties(), properties, 5)) {
			update = true;
		}
		if (update) {
			update(cached);
		}
		return update;
	}

	public boolean maybeUpdateOp(
			final String name,
			final String def,
			final String description,
			final Map<String, Object> properties,
			final ObservedProperty cached) throws ServiceFailureException {
		boolean update = false;
		if (!name.equals(cached.getName())) {
			update = true;
			cached.setName(name);
		}
		if (!description.equals(cached.getDescription())) {
			update = true;
			cached.setDescription(description);
		}
		if (cached.getProperties() == null && properties != null) {
			cached.setProperties(properties);
			update = true;
		}
		if (addProperties(cached.getProperties(), properties, 5)) {
			update = true;
		}
		if (update) {
			update(cached);
		}
		return update;
	}

	public ObservedProperty findOrCreateOp(
			final String filter,
			final String name,
			final String def,
			final String description,
			final Map<String, Object> properties,
			final ObservedProperty cached) throws ServiceFailureException {
		ObservedProperty op = null;
		if (cached != null) {
			op = cached;
		} else {
			final Query<ObservedProperty> query = service.observedProperties().query();
			final EntityList<ObservedProperty> opList = addOrCreateFilter(query, filter, name).list();
			if (opList.size() > 1) {
				throw new IllegalStateException("More than one observedProperty with name " + name);
			}
			if (opList.size() == 1) {
				op = opList.iterator().next();
			}
		}
		if (op == null) {
			LOGGER.info("Creating ObservedProperty {}.", name);
			op = new ObservedProperty();
			op.setName(name);
			op.setDefinition(def);
			op.setDescription(description);
			op.setProperties(properties);
			create(op);
		} else {
			maybeUpdateOp(name, def, description, properties, op);
		}
		return op;
	}

	public boolean maybeUpdateDatastream(
			final String name,
			final String desc,
			final Map<String, Object> properties,
			final UnitOfMeasurement uom,
			final Thing t,
			final ObservedProperty op,
			final Sensor s,
			final Datastream cached) throws ServiceFailureException {
		boolean update = false;
		if (!name.equals(cached.getName())) {
			update = true;
			cached.setName(name);
		}
		if (!desc.equals(cached.getDescription())) {
			update = true;
			cached.setDescription(desc);
		}
		if (cached.getProperties() == null && properties != null) {
			cached.setProperties(properties);
			update = true;
		}
		if (addProperties(cached.getProperties(), properties, 5)) {
			update = true;
		}
		if (!uom.equals(cached.getUnitOfMeasurement())) {
			cached.setUnitOfMeasurement(uom);
			update = true;
		}
		if (update) {
			update(cached);
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
		Datastream ds = null;
		if (cached != null) {
			ds = cached;
		} else {
			final Query<Datastream> query = service.datastreams().query();
			final EntityList<Datastream> datastreamList = addOrCreateFilter(query, filter, name).list();
			if (datastreamList.size() > 1) {
				throw new IllegalStateException("More than one datastream matches filter " + filter);
			}
			if (datastreamList.size() == 1) {
				ds = datastreamList.iterator().next();
			}
		}

		if (ds == null) {
			LOGGER.info("Creating Datastream {}.", name);
			ds = new Datastream(name, desc, "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement", uom);
			ds.setProperties(properties);
			ds.setThing(t);
			ds.setSensor(s);
			ds.setObservedProperty(op);
			create(ds);
		} else {
			maybeUpdateDatastream(name, desc, properties, uom, t, op, s, ds);
		}
		return ds;
	}

	public MultiDatastream findOrCreateMultiDatastream(
			final String filter,
			final String name,
			final String desc,
			final List<UnitOfMeasurement> uoms,
			final Thing t,
			final List<ObservedProperty> ops,
			final Sensor s,
			final Map<String, Object> props,
			final MultiDatastream cached) throws ServiceFailureException {
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
			final List<String> dataTypes = new ArrayList<>();
			for (final ObservedProperty op : ops) {
				dataTypes.add("http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement");
			}
			mds = new MultiDatastream(name, desc, dataTypes, uoms);
			mds.setProperties(props);
			mds.setThing(t);
			mds.setSensor(s);
			mds.getObservedProperties().addAll(ops);
			create(mds);
		} else {
			maybeUpdateMultiDatastream(name, desc, props, mds);
		}
		return mds;
	}

	public boolean maybeUpdateMultiDatastream(
			final String name,
			final String desc,
			final Map<String, Object> props,
			final MultiDatastream cached) throws ServiceFailureException {
		boolean update = false;
		if (!name.equals(cached.getName())) {
			update = true;
			cached.setName(name);
		}
		if (!desc.equals(cached.getDescription())) {
			update = true;
			cached.setDescription(desc);
		}
		if (cached.getProperties() == null && props != null) {
			cached.setProperties(props);
			update = true;
		}
		if (addProperties(cached.getProperties(), props, 5)) {
			update = true;
		}
		if (update) {
			update(cached);
		}
		return update;
	}

	public Location findOrCreateLocation(
			final String name,
			final String description,
			final Map<String, Object> properties,
			final GeoJsonObject geoJson) throws ServiceFailureException {
		String filter = "name eq '" + Utils.escapeForStringConstant(name) + "'";
		return findOrCreateLocation(filter, name, description, properties, geoJson);
	}

	public Location findOrCreateLocation(
			final String filter,
			final String name,
			final String description,
			final Map<String, Object> properties,
			final GeoJsonObject geoJson) throws ServiceFailureException {
		return findOrCreateLocation(filter, name, description, properties, geoJson, null);
	}

	public Location findOrCreateLocation(
			final String filter,
			final String name,
			final String description,
			final Map<String, Object> properties,
			final GeoJsonObject geoJson,
			final Location cached) throws ServiceFailureException {
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
			LOGGER.info("Creating Location {}.", name);
			location = new Location(name, description, ENCODING_GEOJSON, geoJson);
			location.setProperties(properties);
			create(location);
		} else {
			maybeUpdateLocation(name, description, properties, geoJson, location);
		}
		return location;
	}

	public boolean maybeUpdateLocation(
			final String name,
			final String description,
			final Map<String, Object> properties,
			final GeoJsonObject geoJson,
			final Location cached) throws ServiceFailureException {
		boolean updated = false;
		if (!cached.getName().equals(name)) {
			updated = true;
			cached.setName(name);
		}
		if (!cached.getDescription().equals(description)) {
			updated = true;
			cached.setDescription(description);
		}
		if (addProperties(cached.getProperties(), properties, 10)) {
			updated = true;
		}
		ObjectMapper om = ObjectMapperFactory.get();
		try {
			if (!om.writeValueAsString(geoJson).equals(om.writeValueAsString(cached.getLocation()))) {
				updated = true;
				LOGGER.debug("Location changed from {} to {}", cached.getLocation(), geoJson);
				cached.setLocation(geoJson);
			}
		} catch (JsonProcessingException ex) {
			LOGGER.error("Failed to compare geoJson objects.");
		}
		if (updated) {
			update(cached);
		}
		return updated;
	}

	public static String quoteForUrl(final Object in) {
		if (in instanceof Number) {
			return in.toString();
		}
		return "'" + Utils.escapeForStringConstant(in.toString()) + "'";
	}

	public static <Q extends Entity<Q>> Query<Q> addOrCreateFilter(final Query<Q> query, final String filter, final String name) {
		if (Strings.isNullOrEmpty(filter)) {
			return query.filter("name eq '" + Utils.escapeForStringConstant(name) + "'");
		} else {
			return query.filter(filter);
		}
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
				} else {
					if (!resultCompare(value, tValue)) {
						updated = true;
						target.put(key, value);
					}
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
				return ((BigDecimal) one).equals(new BigDecimal(two.toString()));
			}
			if (two instanceof BigDecimal) {
				return ((BigDecimal) two).equals(new BigDecimal(one.toString()));
			}
			if (one instanceof BigInteger) {
				return ((BigInteger) one).equals(new BigInteger(two.toString()));
			}
			if (two instanceof BigInteger) {
				return ((BigInteger) two).equals(new BigInteger(one.toString()));
			}
			if (one instanceof Collection && two instanceof Collection) {
				final Collection cOne = (Collection) one;
				final Collection cTwo = (Collection) two;
				final Iterator iTwo = cTwo.iterator();
				for (final Object itemOne : cOne) {
					if (!iTwo.hasNext()) {
						// Collection one is longer than two
						return false;
					}
					if (!resultCompare(itemOne, iTwo.next())) {
						return false;
					}
				}
				if (iTwo.hasNext()) {
					// Collection two is longer than one.
					return false;
				}
				return true;
			}
		} catch (final NumberFormatException e) {
			LOGGER.trace("Not both bigdecimal.", e);
			// not both bigDecimal.
		}
		return false;
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

	public static DirectPosition2D convertCoordinates(String locationPos, String locationSrsName) throws FactoryException, TransformException, NumberFormatException, MismatchedDimensionException {
		String[] coordinates = locationPos.split(" ");
		CoordinateReferenceSystem sourceCrs = CRS.decode(locationSrsName);
		CoordinateReferenceSystem targetCrs = CRS.decode("EPSG:4326");
		MathTransform transform = CRS.findMathTransform(sourceCrs, targetCrs);
		DirectPosition2D sourcePoint = new DirectPosition2D(sourceCrs, Double.parseDouble(coordinates[1]), Double.parseDouble(coordinates[0]));
		DirectPosition2D targetPoint = new DirectPosition2D(targetCrs);
		transform.transform(sourcePoint, targetPoint);
		return targetPoint;
	}

	public static Map<String, Object> putIntoSubMap(Map<String, Object> map, String subMapName, String key, Object value) {
		Map<String, Object> subMap = (Map<String, Object>) map.computeIfAbsent(subMapName, (String t) -> new HashMap<>());
		subMap.put(key, value);
		return subMap;
	}
}
