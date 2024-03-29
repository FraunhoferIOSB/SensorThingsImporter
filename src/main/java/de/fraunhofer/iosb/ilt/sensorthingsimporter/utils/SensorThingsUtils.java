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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.utils;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.Utils;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Id;
import de.fraunhofer.iosb.ilt.sta.model.Location;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import de.fraunhofer.iosb.ilt.sta.model.Sensor;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.model.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.sta.query.Query;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.geojson.GeoJsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated use FrostUtils instead.
 * @author scf
 */
@Deprecated(forRemoval = true)
public class SensorThingsUtils {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(SensorThingsUtils.class);

	public static enum AggregationLevels {
		HOURLY("[1 hour]", "Aggregated hourly"),
		DAILY("[1 day]", "Aggregated daily");

		public final String postfix;
		public final String description;

		private AggregationLevels(String postfix, String description) {
			this.postfix = postfix;
			this.description = description;
		}

	}

	public static final List<Thing> things = new ArrayList<>();
	public static final List<Location> locations = new ArrayList<>();
	public static final List<Sensor> sensors = new ArrayList<>();
	public static final List<ObservedProperty> oProps = new ArrayList<>();
	public static final Map<ObservedProperty, List<ObservedProperty>> aggregateProperties = new HashMap<>();
	public static final List<Datastream> datastreams = new ArrayList<>();
	public static final List<MultiDatastream> multiDatastreams = new ArrayList<>();
	public static final List<Observation> observations = new ArrayList<>();

	public static final Map<String, Integer> obsPropIds = new HashMap<>();

	public static Sensor findOrCreateSensor(SensorThingsService service, String name, String description) throws ServiceFailureException {
		EntityList<Sensor> sensorList = service.sensors().query().filter("name eq '" + name + "'").list();
		if (sensorList.size() > 1) {
			throw new IllegalStateException("More than one sensor with name " + name);
		}
		Sensor sensor;
		if (sensorList.size() == 1) {
			sensor = sensorList.iterator().next();
		} else {
			LOGGER.info("Creating Sensor {}.", name);
			sensor = new Sensor(name, description, "text", "Properties not known");
			service.create(sensor);
		}
		sensors.add(sensor);
		return sensor;
	}

	public static ObservedProperty findOrCreateOp(SensorThingsService service, String name, URI def, String description, Map<String, Object> properties) throws ServiceFailureException {
		return findOrCreateOp(service, name, def, description, properties, "", false);
	}

	public static ObservedProperty findOrCreateOp(SensorThingsService service, String name, URI def, String description, Map<String, Object> properties, String filter, boolean aggregates) throws ServiceFailureException {
		Query<ObservedProperty> query = service.observedProperties().query();
		if (Utils.isNullOrEmpty(filter)) {
			query.filter("name eq '" + Utils.escapeForStringConstant(name) + "'");
		} else {
			query.filter(filter);
		}
		EntityList<ObservedProperty> opList = query.list();
		if (opList.size() > 1) {
			throw new IllegalStateException("More than one observedProperty with name " + name);
		}
		ObservedProperty op;
		if (opList.size() == 1) {
			op = opList.iterator().next();
		} else {
			LOGGER.info("Creating ObservedProperty {}.", name);
			op = new ObservedProperty(name, def, description);
			op.setProperties(properties);
			service.create(op);
		}
		oProps.add(op);
		if (aggregates) {
			findOrCreateAggregateOps(service, op, def);
		}
		return op;
	}

	public static void findOrCreateAggregateOps(SensorThingsService service, ObservedProperty op, URI def) throws ServiceFailureException {
		List<ObservedProperty> agList = aggregateProperties.get(op);
		if (agList == null) {
			agList = new ArrayList<>();
			aggregateProperties.put(op, agList);
		}
		String opName = op.getName();
		String opDef = op.getDefinition();
		String opDesc = op.getDescription();
		{
			String agOpName = opName + " Min";
			String agOpDesc = opDesc + " Minimum";
			ObservedProperty agOp = findOrCreateOp(service, agOpName, def, agOpDesc, null, "", false);
			agList.add(agOp);
		}
		{
			String agOpName = opName + " Max";
			String agOpDesc = opDesc + " Maximum";
			ObservedProperty agOp = findOrCreateOp(service, agOpName, def, agOpDesc, null, "", false);
			agList.add(agOp);
		}
		{
			String agOpName = opName + " Dev";
			String agOpDesc = opDesc + " Standard deviation";
			ObservedProperty agOp = findOrCreateOp(service, agOpName, def, agOpDesc, null, "", false);
			agList.add(agOp);
		}
	}

	public static Datastream findOrCreateDatastream(SensorThingsService service, String dsName, String dsDescription, Map<String, Object> properties, Thing t, ObservedProperty op, UnitOfMeasurement uom, Sensor s, AggregationLevels... aggregates) throws ServiceFailureException, IOException {
		Datastream ds = findOrCreateDatastream(
				service,
				dsName,
				dsDescription,
				properties,
				uom, t, op, s);

		if (aggregates.length > 0) {
			List<ObservedProperty> ops = new ArrayList<>();
			ops.add(op);
			ops.addAll(aggregateProperties.get(op));
			List<UnitOfMeasurement> uoms = new ArrayList<>();
			for (ObservedProperty tempOp : ops) {
				uoms.add(uom);
			}
			Map<String, Object> aggProps = new HashMap<>();
			aggProps.put("aggregateFor", "/Datastreams(" + ds.getId().getUrl() + ")");
			for (AggregationLevels level : aggregates) {
				String mdsName = dsName + " " + level.postfix;
				String mdsDesc = dsDescription + " " + level.description;
				MultiDatastream mds = findOrCreateMultiDatastream(service, mdsName, mdsDesc, uoms, t, ops, s, aggProps);
				aggProps.put("aggregateFor", "/MultiDatastreams(" + mds.getId().getUrl() + ")");
			}
		}
		return ds;
	}

	public static Datastream findOrCreateDatastream(SensorThingsService service, String name, String desc, Map<String, Object> properties, UnitOfMeasurement uom, Thing t, ObservedProperty op, Sensor s) throws ServiceFailureException {
		EntityList<Datastream> datastreamList = service.datastreams().query().filter("name eq '" + Utils.escapeForStringConstant(name) + "'").list();
		if (datastreamList.size() > 1) {
			throw new IllegalStateException("More than one datastream with name " + name);
		}
		Datastream ds;
		if (datastreamList.size() == 1) {
			ds = datastreamList.iterator().next();
			if (properties != null && addAllToMap(ds.getProperties(), properties)) {
				service.update(ds);
			}
		} else {
			LOGGER.info("Creating Datastream {}.", name);
			ds = new Datastream(
					name,
					desc,
					"http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
					uom);
			ds.setProperties(properties);
			ds.setThing(t);
			ds.setSensor(s);
			ds.setObservedProperty(op);
			service.create(ds);
		}
		datastreams.add(ds);
		return ds;
	}

	public static MultiDatastream findOrCreateMultiDatastream(SensorThingsService service, String name, String desc, List<UnitOfMeasurement> uoms, Thing t, List<ObservedProperty> ops, Sensor s, Map<String, Object> props, AggregationLevels... aggregates) throws ServiceFailureException {
		MultiDatastream mds = findOrCreateMultiDatastream(service, name, desc, uoms, t, ops, s, props);
		Id lastId = mds.getId();
		if (aggregates.length > 0) {
			for (AggregationLevels level : aggregates) {
				Map<String, Object> aggProps = new HashMap<>();
				aggProps.put("aggregateFor", "/MultiDatastreams(" + lastId.getUrl() + ")");
				String mdsName = mds.getName() + " " + level.postfix;
				String mdsDesc = mds.getDescription() + " " + level.description;
				lastId = findOrCreateMultiDatastream(service, mdsName, mdsDesc, uoms, t, ops, s, aggProps).getId();
			}
		}
		return mds;
	}

	public static MultiDatastream findOrCreateMultiDatastream(SensorThingsService service, String name, String desc, List<UnitOfMeasurement> uoms, Thing t, List<ObservedProperty> ops, Sensor s, Map<String, Object> props) throws ServiceFailureException {
		EntityList<MultiDatastream> mdsList = service.multiDatastreams().query().filter("name eq '" + Utils.escapeForStringConstant(name) + "'").list();
		if (mdsList.size() > 1) {
			throw new IllegalStateException("More than one multidatastream with name " + name);
		}
		MultiDatastream mds;
		if (mdsList.size() == 1) {
			mds = mdsList.iterator().next();
		} else {
			LOGGER.info("Creating multiDatastream {}.", name);
			List<String> dataTypes = new ArrayList<>();
			for (ObservedProperty op : ops) {
				dataTypes.add("http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement");
			}
			mds = new MultiDatastream(
					name,
					desc,
					dataTypes,
					uoms);
			mds.setProperties(props);
			mds.setThing(t);
			mds.setSensor(s);
			mds.getObservedProperties().addAll(ops);
			service.create(mds);
		}
		multiDatastreams.add(mds);
		return mds;
	}

	public static Location findOrCreateLocation(SensorThingsService service, String name, String description, GeoJsonObject geoJson) throws ServiceFailureException {
		EntityList<Location> lList = service.locations().query().filter("name eq '" + Utils.escapeForStringConstant(name) + "'").list();
		if (lList.size() > 1) {
			throw new IllegalStateException("More than one Location with name " + name);
		}
		Location location;
		if (lList.size() == 1) {
			location = lList.iterator().next();
		} else {
			LOGGER.info("Creating Location {}.", name);
			location = new Location(
					name,
					description,
					"application/geo+json",
					geoJson);
			service.create(location);
		}
		locations.add(location);
		return location;
	}

	/**
	 *
	 * @param target
	 * @param source
	 * @return true if the target map was changed.
	 */
	public static boolean addAllToMap(Map<String, Object> target, Map<String, Object> source) {
		if (source == null) {
			return false;
		}
		if (target == null) {
			LOGGER.error("Target map is null!");
			return false;
		}
		boolean changed = false;
		for (Map.Entry<String, Object> entry : source.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if (!equals(value, target.get(key))) {
				LOGGER.info("Updated property {}, with value {}.", key, value);
				target.put(key, value);
				changed = true;
			}
		}
		return changed;
	}

	public static boolean equals(Object o1, Object o2) {
		if (o1 instanceof List && o2 instanceof List) {
			return equals((List) o1, (List) o2);
		}
		if (o1 instanceof BigDecimal && o2 instanceof BigDecimal) {
			return o1.equals(o2);
		}
		if (o1 instanceof BigDecimal && o2 instanceof Number) {
			return o1.equals(new BigDecimal(o2.toString()));
		}
		if (o2 instanceof BigDecimal && o1 instanceof Number) {
			return o2.equals(new BigDecimal(o1.toString()));
		}
		return o1.equals(o2);
	}

	public static boolean equals(List o1, List o2) {
		if (o1.size() != o2.size()) {
			return false;
		}
		for (int i = 0; i < o1.size(); i++) {
			if (!equals(o1.get(i), o2.get(i))) {
				return false;
			}
		}
		return true;
	}
}
