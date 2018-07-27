/*
 * Copyright (C) 2018 Fraunhofer IOSB
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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.importers;

import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.AbstractConfigurable;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Importer;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.SensorThingsUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.SensorThingsUtils.AggregationLevels;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Entity;
import de.fraunhofer.iosb.ilt.sta.model.Location;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import de.fraunhofer.iosb.ilt.sta.model.Sensor;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.TimeObject;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.model.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.dbcp2.BasicDataSource;
import org.geojson.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: Make multiDatastreams for:
 *
 * 11 - wind speed 10 - wind direction
 *
 * 3,4,5 Temp mean, high, low
 *
 * 31,32,33 Temp avg, low, high
 *
 * @author scf
 */
public class ImporterSistemaPostgresDb extends AbstractConfigurable<SensorThingsService, Object> implements Importer {

	private static final String CROSS_PRODUCT_QUERY = "select distinct m.observed_field,m.stations_id from measurements m order by m.observed_field,m.stations_id;";
	private static final String QUERY_OBS_SINGLE_PRODUCT = "select * from measurements where stations_id=? and observed_field=? order by observation_time_start asc, id desc;";
	private static final String QUERY_OBS_SINGLE_PRODUCT_WITHDATE = "select * from measurements where stations_id=? and observed_field=? and observation_time_start > ? order by observation_time_start asc, id desc;";
	private static final String QUERY_OBS_MULTI_PRODUCT = "select * from measurements where stations_id=? and observed_field=any(?) order by observation_time_start desc;";
	private static final String QUERY_OBS_MULTI_PRODUCT_WITHDATE = "select * from measurements where stations_id=? and observed_field=any(?) and observation_time_start > ? order by observation_time_start asc, id desc;";
	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ImporterSistemaPostgresDb.class);
	private static final UnitOfMeasurement NULL_UNIT = new UnitOfMeasurement("-", "-", "-");

	private static final String TAG_SISTEMA_ID = "sistemaId";
	private static final String TAG_SISTEMA_IDS = "sistemaIds";
	private static final String TAG_SISTEMA_UNITS = "sistemaUnits";
	private static final String TAG_SISTEMA_PRODUCTS = "sistemaProducts";
	private static final String TAG_SISTEMA_STATION = "sistemaStation";

	@ConfigurableField(editor = EditorString.class,
			label = "Driver",
			description = "The database driver class to use.")
	@EditorString.EdOptsString(dflt = "org.postgresql.Driver")
	private String dbDriver;

	@ConfigurableField(editor = EditorString.class,
			label = "Url",
			description = "The database connection url.")
	@EditorString.EdOptsString(dflt = "jdbc:postgresql://hostname:5432/database")
	private String dbUrl;

	@ConfigurableField(editor = EditorString.class,
			label = "Username",
			description = "The database username to use.")
	@EditorString.EdOptsString(dflt = "myUserName")
	private String dbUsername;

	@ConfigurableField(editor = EditorString.class,
			label = "Password",
			description = "The database password to use.")
	@EditorString.EdOptsString(dflt = "myPassword")
	private String dbPassword;

	@ConfigurableField(editor = EditorString.class,
			label = "NaN Value",
			description = "The value the service used that should be ignored.")
	@EditorString.EdOptsString(dflt = "9999")
	private String nanValue;

	@ConfigurableField(editor = EditorBoolean.class,
			label = "Full Import",
			description = "Check the entire meta-data model each time.")
	@EditorBoolean.EdOptsBool(dflt = true)
	private Boolean fullImport;

	private boolean noAct = false;
	private boolean verbose = true;

	private SensorThingsService service;

	/**
	 * The things for the stations, by external-station-id
	 */
	private Map<Integer, Thing> stations = new HashMap<>();
	private Map<Integer, ObservedProperty> observedProperties = new HashMap<>();
	private Map<Integer, Map<Integer, Datastream>> datastreamsByOpByStation = new HashMap<>();
	private Map<Integer, Map<Integer, MultiDatastream>> multiDatastreamsByOpByStation = new HashMap<>();
	private Map<String, UnitOfMeasurement> unitMapping;
	private Map<String, String> observedPropertyNameMapping;

	private Sensor unknownSensor;
	private Set<Integer> ignoreProducts = new HashSet<>(Arrays.asList(
			1, 2, 7, 22, 25, 26, 36, 24, 41
	));

	private static class MultiDsSet {

		int[] productIds;
		String name;
		boolean aggregates;

		public MultiDsSet(int[] productIds, String name, boolean aggregates) {
			this.productIds = productIds;
			this.name = name;
			this.aggregates = aggregates;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final MultiDsSet other = (MultiDsSet) obj;
			if (!Objects.equals(this.name, other.name)) {
				return false;
			}
			if (!Arrays.equals(this.productIds, other.productIds)) {
				return false;
			}
			return true;
		}

		@Override
		public int hashCode() {
			int hash = 7;
			hash = 67 * hash + Arrays.hashCode(this.productIds);
			hash = 67 * hash + Objects.hashCode(this.name);
			return hash;
		}

	}
	private Map<Integer, MultiDsSet> multiDatastreams = new HashMap<>();
	private String noResult = "9999";

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx) {
		super.configure(config, context, edtCtx);
		service = context;
	}

	@Override
	public void setNoAct(boolean noAct) {
		this.noAct = noAct;
	}

	@Override
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	@Override
	public Iterator<List<Observation>> iterator() {
		try {
			ObsListIter obsListIter = new ObsListIter();
			return obsListIter;
		} catch (SQLException | ServiceFailureException | URISyntaxException | IOException exc) {
			LOGGER.error("Failed", exc);
			throw new IllegalStateException("Failed to handle csv file.", exc);
		}
	}

	private void initMappings() {
		unitMapping = new HashMap<>();
		unitMapping.put("C", new UnitOfMeasurement("degree celcius", "°C", "ucum:Cel"));
		unitMapping.put("mbar", new UnitOfMeasurement("millibar", "mbar", "ucum:mbar"));
		unitMapping.put("ug/m3", new UnitOfMeasurement("µg/m3", "µg/m3", "ucum:ug/m3"));
		unitMapping.put("mm/h", new UnitOfMeasurement("mm/h", "mm/h", "ucum:mm/h"));
		unitMapping.put("km/h", new UnitOfMeasurement("km/h", "km/h", "ucum:km/h"));
		unitMapping.put("km/hr", new UnitOfMeasurement("km/h", "km/h", "ucum:km/h"));
		unitMapping.put("knots", new UnitOfMeasurement("knots", "knots", "ucum:knots"));
		unitMapping.put("celsius degree", unitMapping.get("C"));
		unitMapping.put("m/s", new UnitOfMeasurement("m/s", "m/s", "ucum:m/s"));
		unitMapping.put("mg/m3", new UnitOfMeasurement("mg/m3", "mg/m3", "ucum:mg/m3"));
		unitMapping.put("degree", new UnitOfMeasurement("DegreesOfArc", "deg", "ucum:deg"));
		unitMapping.put("mW/cm2", new UnitOfMeasurement("milliwat per square cm", "mW/cm2", "ucum:mW/cm2"));
		unitMapping.put("%", new UnitOfMeasurement("percent", "%", "ucum:%"));
		unitMapping.put("hPa", unitMapping.get("mbar"));
		unitMapping.put("mm", unitMapping.get("mm/h"));

		observedPropertyNameMapping = new HashMap<>();
		observedPropertyNameMapping.put("PRESS".toUpperCase(), "Air Pressure");
		observedPropertyNameMapping.put("Pressure".toUpperCase(), "Air Pressure");

		observedPropertyNameMapping.put("temperature".toUpperCase(), "Air Temperature");
		observedPropertyNameMapping.put("Temp".toUpperCase(), "Air Temperature");

		observedPropertyNameMapping.put("T mean".toUpperCase(), "Air Temperature");
		observedPropertyNameMapping.put("T Max".toUpperCase(), "Air Temperature Max");
		observedPropertyNameMapping.put("T Min".toUpperCase(), "Air Temperature Min");

		observedPropertyNameMapping.put("Temp Mean".toUpperCase(), "Air Temperature");
		observedPropertyNameMapping.put("Temp high".toUpperCase(), "Air Temperature Max");
		observedPropertyNameMapping.put("Temp low".toUpperCase(), "Air Temperature Min");

		observedPropertyNameMapping.put("rain".toUpperCase(), "Precipitation");
		observedPropertyNameMapping.put("pioggia".toUpperCase(), "Precipitation");
		observedPropertyNameMapping.put("dew point".toUpperCase(), "Dewpoint");
		observedPropertyNameMapping.put("wind".toUpperCase(), "Wind Velocity");
		observedPropertyNameMapping.put("wind speed".toUpperCase(), "Wind Velocity");
		observedPropertyNameMapping.put("wind avg speed".toUpperCase(), "Wind Velocity");
		observedPropertyNameMapping.put("dom dir".toUpperCase(), "Wind Direction");
		observedPropertyNameMapping.put("wind direction".toUpperCase(), "Wind Direction");

		addMultiDsOpList(new MultiDsSet(new int[]{3, 5, 4}, "Air Temperature", true));
		addMultiDsOpList(new MultiDsSet(new int[]{31, 32, 33}, "Air Temperature", true));
		addMultiDsOpList(new MultiDsSet(new int[]{11, 10}, "Wind", false));
		addMultiDsOpList(new MultiDsSet(new int[]{8, 9}, "Wind", false));
	}

	private void addMultiDsOpList(MultiDsSet set) {
		for (Integer prodId : set.productIds) {
			multiDatastreams.put(prodId, set);
		}
	}

	private UnitOfMeasurement getUnitFor(String external) {
		if (unitMapping == null) {
			initMappings();
		}
		UnitOfMeasurement unit = unitMapping.get(external);
		if (unit == null) {
			return NULL_UNIT;
		}
		return unit;
	}

	private String getObservedPropertyNameFor(String sistemaName) {
		String internalName = observedPropertyNameMapping.get(sistemaName.toUpperCase());
		if (Strings.isNullOrEmpty(internalName)) {
			return sistemaName;
		}
		return internalName;
	}

	private class ObsListIter extends AbstractIterator<List<Observation>> {

		private boolean initialised = false;
		private BasicDataSource dataSource;
		private Iterator<Datastream> datastreamIter;
		private Iterator<MultiDatastream> multiDatastreamIter;

		public ObsListIter() throws SQLException, ServiceFailureException, URISyntaxException, IOException {
			initDatabse();
		}

		private void initDatabse() throws SQLException, ServiceFailureException, URISyntaxException, IOException {
			if (initialised) {
				return;
			}
			initialised = true;
			initMappings();
			try {
				Class.forName(dbDriver);
			} catch (ClassNotFoundException ex) {
				LOGGER.error("Could not initialise database.", ex);
				throw new IllegalArgumentException(ex);
			}

			dataSource = new BasicDataSource();
			dataSource.setUrl(dbUrl);
			dataSource.setUsername(dbUsername);
			dataSource.setPassword(dbPassword);

			if (fullImport) {
				unknownSensor = SensorThingsUtils.findOrCreateSensor(service, "unknown", "An unknown sensor type.");
				generateObservedProperties();
				generateStations();
				generateDatastreams();
			}
			datastreamIter = getDatastreamIterator();
			multiDatastreamIter = getMultiDatastreamIterator();
		}

		private void generateObservedProperties() throws ServiceFailureException, URISyntaxException {
			Connection connection = null;
			try {
				connection = dataSource.getConnection();
				String queryString = "select * from products;";
				PreparedStatement statement = connection.prepareStatement(queryString);
				ResultSet resultSet = statement.executeQuery();
				while (resultSet.next()) {
					int id = resultSet.getInt("id");
					if (ignoreProducts.contains(id)) {
						continue;
					}
					String sistemaName = resultSet.getString("product");
					String internalName = getObservedPropertyNameFor(sistemaName);
					String unit = resultSet.getString("measurement_unit");
					ObservedProperty observedProperty = findObservedProperty(id);
					if (observedProperty == null) {
						generateObservedProperty(id, internalName, unit);
					} else {
						generateObservedProperty(id, observedProperty.getName(), unit);
					}
				}
			} catch (SQLException ex) {
				LOGGER.error("Database error!");
				throw new RuntimeException(ex);
			} finally {
				if (connection != null) {
					try {
						connection.close();
					} catch (SQLException ex) {
						LOGGER.error("Failed to close database connection!");
						throw new RuntimeException(ex);
					}
				}
			}
		}

		private ObservedProperty findObservedProperty(int id) throws ServiceFailureException {
			if (observedProperties.isEmpty()) {
				EntityList<ObservedProperty> obsProps = service.observedProperties()
						.query()
						.top(1000)
						.list();
				Iterator<ObservedProperty> it;
				for (it = obsProps.fullIterator(); it.hasNext();) {
					ObservedProperty op = it.next();
					Map<String, Object> props = op.getProperties();
					if (props == null) {
						continue;
					}
					Object idsObject = props.get(TAG_SISTEMA_IDS);
					if (idsObject instanceof List) {
						List list = (List) idsObject;
						for (Object sistemaId : list) {
							if (sistemaId instanceof Number) {
								observedProperties.put(((Number) sistemaId).intValue(), op);
							}
						}
					}
				}
			}
			return observedProperties.get(id);
		}

		private void generateObservedProperty(int id, String name, String unit) throws URISyntaxException, ServiceFailureException {
			Map<String, Object> props = new HashMap<>();
			props.put(TAG_SISTEMA_IDS, Arrays.asList(id));
			props.put(TAG_SISTEMA_UNITS, Arrays.asList(unit));

			ObservedProperty op = SensorThingsUtils.findOrCreateOp(
					service,
					name,
					new URI("http://sistema.org/unknown"),
					name + " in " + unit,
					props,
					null,
					false);
			boolean update = false;
			Map<String, Object> opProps = op.getProperties();
			if (opProps == null) {
				opProps = new HashMap<>();
				op.setProperties(opProps);
				update = true;
			}
			if (!opProps.containsKey(TAG_SISTEMA_IDS)) {
				opProps.put(TAG_SISTEMA_IDS, new ArrayList<>());
				opProps.put(TAG_SISTEMA_UNITS, new ArrayList<>());
				update = true;
			}
			if (!((List) opProps.get(TAG_SISTEMA_IDS)).contains(id)) {
				((List) opProps.get(TAG_SISTEMA_IDS)).add(id);
				((List) opProps.get(TAG_SISTEMA_UNITS)).add(unit);
				update = true;
			}
			if (update) {
				service.update(op);
			}
			observedProperties.put(id, op);
		}

		private void generateStations() throws ServiceFailureException {
			Connection connection = null;
			try {
				connection = dataSource.getConnection();
				String queryString = "select * from stations;";
				PreparedStatement statement = connection.prepareStatement(queryString);
				ResultSet resultSet = statement.executeQuery();
				while (resultSet.next()) {
					int id = resultSet.getInt("id");
					String name = resultSet.getString("name");
					double lat = resultSet.getDouble("latitude");
					double lon = resultSet.getDouble("longitude");
					if (lat > 90 || lat < -90 || lon > 180 || lon < -180) {
						LOGGER.info("Invalid coordinates for station {} {}: {},{}", id, name, lat, lon);
						continue;
					}
					EntityList<Thing> things = service.things()
							.query()
							.filter("properties/type eq 'station' and properties/sistemaId eq " + id)
							.top(2)
							.list();
					if (things.size() > 1) {
						throw new IllegalStateException("Got multiple stations with sistemaId " + id);
					} else if (things.size() == 1) {
						stations.put(id, things.toList().get(0));
					} else {
						generateStation(id, name, lat, lon);
					}
				}
			} catch (SQLException ex) {
				LOGGER.error("Database error!");
				throw new RuntimeException(ex);
			} finally {
				if (connection != null) {
					try {
						connection.close();
					} catch (SQLException ex) {
						LOGGER.error("Failed to close database connection!");
						throw new RuntimeException(ex);
					}
				}
			}
		}

		private void generateStation(int id, String name, double lat, double lon) throws ServiceFailureException {
			Thing thing = new Thing(name, "sistema imported station");
			Location location = new Location(
					name,
					"Location of station " + name,
					"application/geo+json",
					new Point(lon, lat));
			thing.getLocations().add(location);
			Map<String, Object> props = new HashMap<>();
			props.put("type", "station");
			props.put(TAG_SISTEMA_ID, id);
			thing.setProperties(props);

			if (noAct) {
				LOGGER.warn("Not creating {}", thing);
			} else {
				service.create(thing);
				stations.put(id, thing);
				LOGGER.info("Created station {}.", thing);
			}
		}

		private void generateDatastreams() throws ServiceFailureException, IOException {
			Connection connection = null;
			try {
				connection = dataSource.getConnection();
				PreparedStatement statement = connection.prepareStatement(CROSS_PRODUCT_QUERY);
				ResultSet resultSet = statement.executeQuery();
				while (resultSet.next()) {
					int observed_field = resultSet.getInt("observed_field");
					int stations_id = resultSet.getInt("stations_id");
					try {
						findDsMds(stations_id, observed_field);
					} catch (IllegalStateException exc) {
						LOGGER.warn("Ignoring combination stations_id {}, observed_field {}.", stations_id, observed_field);
					}
				}
			} catch (SQLException ex) {
				LOGGER.error("Database error!");
				throw new RuntimeException(ex);
			} finally {
				if (connection != null) {
					try {
						connection.close();
					} catch (SQLException ex) {
						LOGGER.error("Failed to close connection!");
						throw new RuntimeException(ex);
					}
				}
			}
		}

		private Entity findDsMds(int stationId, int propertyId) throws ServiceFailureException, IOException {
			if (multiDatastreams.containsKey(propertyId)) {
				return findMultiDatastream(stationId, propertyId);
			}
			return findDatastream(stationId, propertyId);
		}

		private Entity findDatastream(int stationId, int propertyId) throws ServiceFailureException, IOException {

			Map<Integer, Datastream> props = datastreamsByOpByStation.get(stationId);
			if (props == null) {
				props = new HashMap<>();
				datastreamsByOpByStation.put(stationId, props);
			}

			Datastream datastream = props.get(propertyId);
			if (datastream == null) {
				datastream = generateDatastreamFor(stationId, propertyId);
				props.put(propertyId, datastream);
			}
			return datastream;
		}

		private Entity findMultiDatastream(int stationId, int propertyId) throws ServiceFailureException, IOException {
			Map<Integer, MultiDatastream> props = multiDatastreamsByOpByStation.get(stationId);
			if (props == null) {
				props = new HashMap<>();
				multiDatastreamsByOpByStation.put(stationId, props);
			}

			MultiDatastream datastream = props.get(propertyId);
			if (datastream == null) {
				datastream = generateMultiDatastreamFor(stationId, multiDatastreams.get(propertyId));
				props.put(propertyId, datastream);
			}
			return datastream;
		}

		private MultiDatastream generateMultiDatastreamFor(int stationId, MultiDsSet multiDsSet) throws ServiceFailureException, IOException {
			Thing station = stations.get(stationId);
			if (station == null) {
				throw new IllegalStateException("Unknown station");
			}
			List<ObservedProperty> obsProps = new ArrayList<>();
			List<UnitOfMeasurement> uoms = new ArrayList<>();
			StringBuilder sistemaProductsBuilder = new StringBuilder('-');
			for (int sistemaPropertyId : multiDsSet.productIds) {
				ObservedProperty observedProperty = observedProperties.get(sistemaPropertyId);
				if (observedProperty == null) {
					throw new IllegalStateException("Unknown obsProp");
				}
				obsProps.add(observedProperty);
				sistemaProductsBuilder.append(sistemaPropertyId).append("-");
				uoms.add(getUnitFor(getSistemaUnitFromObservedProperty(observedProperty, sistemaPropertyId)));
			}
			String sistemaProducts = sistemaProductsBuilder.toString();
			MultiDatastream datastream = station.multiDatastreams()
					.query()
					.filter("properties/sistemaProducts eq '" + sistemaProducts + "'")
					.first();
			if (datastream != null) {
				return datastream;
			}
			String dsName = multiDsSet.name + " at " + station.getName();
			if (noAct) {
				LOGGER.warn("Not creating datastream {}", dsName);
				return null;
			}
			Map<String, Object> properties = new HashMap<>();
			properties.put(TAG_SISTEMA_PRODUCTS, sistemaProducts);
			properties.put(TAG_SISTEMA_STATION, stationId);
			AggregationLevels[] levels;
			if (multiDsSet.aggregates) {
				levels = new AggregationLevels[]{AggregationLevels.DAILY};
			} else {
				levels = new AggregationLevels[0];
			}
			datastream = SensorThingsUtils.findOrCreateMultiDatastream(
					service,
					dsName,
					dsName,
					uoms,
					station,
					obsProps,
					unknownSensor,
					properties,
					levels);
			return datastream;
		}

		private String getSistemaUnitFromObservedProperty(ObservedProperty observedProperty, int sistemaId) {
			Map<String, Object> properties = observedProperty.getProperties();
			if (properties == null) {
				return null;
			}
			Object sistemaIds = properties.get(TAG_SISTEMA_IDS);
			Object sistemaUnits = properties.get(TAG_SISTEMA_UNITS);
			if (sistemaIds == null || sistemaUnits == null) {
				return null;
			}
			List listIds = (List) sistemaIds;
			List listUnits = (List) sistemaUnits;
			int idx = listIds.indexOf(sistemaId);
			if (idx < 0 || idx >= listUnits.size()) {
				return null;
			}
			Object unit = listUnits.get(idx);
			if (unit == null) {
				return null;
			}
			return unit.toString();
		}

		private Datastream generateDatastreamFor(int stationId, int propertyId) throws ServiceFailureException, IOException {
			Thing station = stations.get(stationId);
			ObservedProperty observedProperty = observedProperties.get(propertyId);
			if (station == null || observedProperty == null) {
				throw new IllegalStateException("Unknown station or obsProp");
			}
			String unit = getSistemaUnitFromObservedProperty(observedProperty, propertyId);
			Datastream datastream = station.datastreams()
					.query()
					.filter("ObservedProperty/id eq " + observedProperty.getId().getUrl())
					.first();
			if (datastream != null) {
				return datastream;
			}
			String dsName = observedProperty.getName() + " at " + station.getName();
			if (noAct) {
				LOGGER.warn("Not creating datastream {}", dsName);
				return null;
			}
			Map<String, Object> properties = new HashMap<>();
			properties.put(TAG_SISTEMA_PRODUCTS, propertyId);
			properties.put(TAG_SISTEMA_STATION, stationId);
			datastream = SensorThingsUtils.findOrCreateDatastream(
					service,
					dsName,
					dsName,
					properties,
					station,
					observedProperty,
					getUnitFor(unit),
					unknownSensor);
			return datastream;
		}

		private Iterator<Datastream> getDatastreamIterator() throws ServiceFailureException {
			return service.datastreams()
					.query()
					.filter("properties/" + TAG_SISTEMA_PRODUCTS + " gt 0")
					.expand("Observations($orderby=phenomenonTime desc;$top=1)")
					.top(1000)
					.orderBy("id asc")
					.list()
					.fullIterator();
		}

		private Iterator<MultiDatastream> getMultiDatastreamIterator() throws ServiceFailureException {
			return service.multiDatastreams()
					.query()
					.filter("length(properties/" + TAG_SISTEMA_PRODUCTS + ") gt 0")
					.expand("Observations($orderby=phenomenonTime desc;$top=1)")
					.top(1000)
					.orderBy("id asc")
					.list()
					.fullIterator();
		}

		private ResultSet queryDatabaseFor(Connection connection, int stationId, int productId) throws SQLException {
			PreparedStatement statement = connection.prepareStatement(QUERY_OBS_SINGLE_PRODUCT);
			statement.setInt(1, stationId);
			statement.setInt(2, productId);
			ResultSet resultSet = statement.executeQuery();
			return resultSet;
		}

		private ResultSet queryDatabaseFor(Connection connection, int stationId, int productId, Instant lastObs) throws SQLException {
			PreparedStatement statement = connection.prepareStatement(QUERY_OBS_SINGLE_PRODUCT_WITHDATE);
			statement.setInt(1, stationId);
			statement.setInt(2, productId);
			statement.setTimestamp(3, Timestamp.from(lastObs));
			ResultSet resultSet = statement.executeQuery();
			return resultSet;
		}

		private ResultSet queryDatabaseFor(Connection connection, int stationId, List<Integer> productId) throws SQLException {
			PreparedStatement statement = connection.prepareStatement(QUERY_OBS_MULTI_PRODUCT);
			statement.setInt(1, stationId);
			statement.setArray(2, connection.createArrayOf("int", productId.toArray()));
			ResultSet resultSet = statement.executeQuery();
			return resultSet;
		}

		private ResultSet queryDatabaseFor(Connection connection, int stationId, List<Integer> productId, Instant lastObs) throws SQLException {
			PreparedStatement statement = connection.prepareStatement(QUERY_OBS_MULTI_PRODUCT_WITHDATE);
			statement.setInt(1, stationId);
			statement.setArray(2, connection.createArrayOf("int", productId.toArray()));
			statement.setTimestamp(3, Timestamp.from(lastObs));
			ResultSet resultSet = statement.executeQuery();
			return resultSet;
		}

		private Object parseResult(String result) {
			try {
				return new BigDecimal(result);
			} catch (NumberFormatException exc) {
				LOGGER.trace("Result not a number: {}", result);
				LOGGER.trace("Result not a number.", exc);
			}
			return result;
		}

		private List<Observation> generateObservationsForDatastream(Connection connection, Datastream datastream) throws SQLException {
			LOGGER.debug("Starting import for {}", datastream);
			Object products = datastream.getProperties().get(TAG_SISTEMA_PRODUCTS);
			int productId = Integer.parseInt(products.toString());
			Object station = datastream.getProperties().get(TAG_SISTEMA_STATION);
			int stationId = Integer.parseInt(station.toString());
			List<Observation> existingObservations = datastream.getObservations().toList();

			List<Observation> resultList = new ArrayList<>();
			ResultSet resultSet;
			if (existingObservations.isEmpty()) {
				resultSet = queryDatabaseFor(connection, stationId, productId);
			} else {
				Instant obsTime = startTimeFromPhenTime(existingObservations.get(0).getPhenomenonTime());
				resultSet = queryDatabaseFor(connection, stationId, productId, obsTime);
			}
			while (resultSet.next()) {
				String result = resultSet.getString("value");
				if (nanValue.equalsIgnoreCase(result)) {
					continue;
				}
				Timestamp timestamp = resultSet.getTimestamp("observation_time_start");
				Observation o = new Observation(parseResult(result), datastream);
				o.setPhenomenonTimeFrom(timestamp.toInstant().atZone(ZoneOffset.UTC));
				resultList.add(o);
			}
			LOGGER.info("Found {} Observations for {} ({}; {}).", resultList.size(), datastream, stationId, productId);
			return resultList;
		}

		private List<Observation> generateObservationsForMultiDatastream(Connection connection, MultiDatastream multiDatastream) throws SQLException {
			LOGGER.debug("Starting import for {}", multiDatastream);
			Object productsObj = multiDatastream.getProperties().get(TAG_SISTEMA_PRODUCTS);
			String[] split = productsObj.toString().split("-");
			List<Integer> products = new ArrayList<>();
			for (String item : split) {
				if (!item.trim().isEmpty()) {
					products.add(Integer.parseInt(item));
				}
			}

			Object station = multiDatastream.getProperties().get(TAG_SISTEMA_STATION);
			int stationId = Integer.parseInt(station.toString());
			List<Observation> existingObservations = multiDatastream.getObservations().toList();

			List<Observation> resultList = new ArrayList<>();
			ResultSet resultSet;
			if (existingObservations.isEmpty()) {
				resultSet = queryDatabaseFor(connection, stationId, products);
			} else {
				Instant obsTime = startTimeFromPhenTime(existingObservations.get(0).getPhenomenonTime());
				resultSet = queryDatabaseFor(connection, stationId, products, obsTime);
			}

			MultiDsSet multiDsSet = multiDatastreams.get(products.get(0));
			Observation inProgressObs = null;
			Object[] inProgressResult = null;
			Timestamp lastTimeStart = null;
			while (resultSet.next()) {
				String dbResult = resultSet.getString("value");
				if (nanValue.equalsIgnoreCase(dbResult)) {
					continue;
				}
				Timestamp dbObsTimeStart = resultSet.getTimestamp("observation_time_start");
				Instant phenTimeInstant = dbObsTimeStart.toInstant();
				int product = resultSet.getInt("observed_field");
				if (inProgressObs != null && !dbObsTimeStart.equals(lastTimeStart)) {
					LOGGER.debug("finishing observation: {}", inProgressObs);
					resultList.add(inProgressObs);
					inProgressObs = null;
				}

				if (inProgressObs == null || inProgressResult == null) {
					inProgressResult = new Object[multiDsSet.productIds.length];
					inProgressObs = new Observation(inProgressResult, multiDatastream);
					inProgressObs.setPhenomenonTime(new TimeObject(phenTimeInstant.atZone(ZoneOffset.UTC)));
				}
				inProgressResult[indexOf(multiDsSet.productIds, product)] = parseResult(dbResult);
				lastTimeStart = dbObsTimeStart;
			}
			LOGGER.info("Found {} Observations for {} ({}; {}).", resultList.size(), multiDatastream, stationId, productsObj);
			return resultList;
		}

		private int indexOf(int[] arr, int item) {
			for (int idx = 0; idx < arr.length; idx++) {
				if (arr[idx] == item) {
					return idx;
				}
			}
			return -1;
		}

		@Override
		protected List<Observation> computeNext() {
			Connection connection = null;
			try {
				connection = dataSource.getConnection();
				if (datastreamIter.hasNext()) {
					return generateObservationsForDatastream(connection, datastreamIter.next());
				}
				if (multiDatastreamIter.hasNext()) {
					return generateObservationsForMultiDatastream(connection, multiDatastreamIter.next());
				}
			} catch (SQLException exc) {
				LOGGER.error("Exception fetching data!", exc);
			} finally {
				if (connection != null) {
					try {
						connection.close();
					} catch (SQLException ex) {
						LOGGER.error("Failed to close connection!");
						throw new RuntimeException(ex);
					}
				}
			}
			return endOfData();
		}

		private Instant startTimeFromPhenTime(TimeObject time) {
			if (time.isInterval()) {
				return time.getAsInterval().getStart();
			} else {
				return time.getAsDateTime().toInstant();
			}
		}
	}
}
