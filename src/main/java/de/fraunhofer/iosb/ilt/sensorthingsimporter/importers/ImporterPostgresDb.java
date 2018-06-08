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

import com.google.common.collect.AbstractIterator;
import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Importer;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.SensorThingsUtils;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Entity;
import de.fraunhofer.iosb.ilt.sta.model.Location;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import de.fraunhofer.iosb.ilt.sta.model.Sensor;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.model.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
public class ImporterPostgresDb implements Importer {

	private static final String CROSS_PRODUCT_QUERY = "select distinct m.observed_field,m.stations_id from measurements m;";
	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ImporterPostgresDb.class);
	private boolean noAct = false;
	private boolean verbose = true;

	private SensorThingsService service;

	private EditorMap<Map<String, Object>> editor;
	private EditorString editorDbDriver;
	private EditorString editorDbUrl;
	private EditorString editorDbUsername;
	private EditorString editorDbPassword;
	private EditorBoolean editorFullImport;

	/**
	 * The things for the stations, by external-station-id
	 */
	private Map<Integer, Thing> stations = new HashMap<>();
	private Map<Integer, ObservedProperty> observedProperties = new HashMap<>();
	private Map<Integer, Map<Integer, Datastream>> datastreamsByOpByStation = new HashMap<>();
	private Map<Integer, Map<Integer, MultiDatastream>> multiDatastreamsByOpByStation = new HashMap<>();
	private Map<String, UnitOfMeasurement> unitMapping;

	private Sensor unknownSensor;
	private Set<Integer> ignoreProducts = new HashSet<>(Arrays.asList(
			1, 2, 22, 24, 41
	));
	private Map<Integer, int[]> multiDsOps = new HashMap<>();
	private String noResult = "9999";

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx) {
		service = context;
		getConfigEditor(context, edtCtx).setConfig(config);
	}

	@Override
	public ConfigEditor<?> getConfigEditor(SensorThingsService context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorMap<>();

			editorDbDriver = new EditorString("org.postgresql.Driver", 1, "Driver", "The database driver class to use.");
			editor.addOption("dbDriver", editorDbDriver, true);

			editorDbUrl = new EditorString("jdbc:postgresql://hostname:5432/database", 1, "Url", "The database connection url.");
			editor.addOption("dbUrl", editorDbUrl, true);

			editorDbUsername = new EditorString("myUserName", 1, "Username", "The database username to use.");
			editor.addOption("dbUsername", editorDbUsername, true);

			editorDbPassword = new EditorString("myPassword", 1, "Password", "The database password to use.");
			editor.addOption("dbPassword", editorDbPassword, true);

			editorFullImport = new EditorBoolean(true, "Full Import", "Import all rivers and sections every time.");
			editor.addOption("fullImport", editorFullImport, true);

		}
		return editor;
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

	private void initUnitMapping() {
		unitMapping = new HashMap<>();
		unitMapping.put(null, new UnitOfMeasurement(null, null, null));
		unitMapping.put("C", new UnitOfMeasurement("degree celcius", "°C", "ucum:Cel"));
		unitMapping.put("mbar", new UnitOfMeasurement("millibar", "mbar", "ucum:mbar"));
		unitMapping.put("ug/m3", new UnitOfMeasurement("µg/m3", "µg/m3", "ucum:ug/m3"));
		unitMapping.put("mm/h", new UnitOfMeasurement("mm/h", "mm/h", "ucum:mm/h"));
		unitMapping.put("km/h", new UnitOfMeasurement("km/h", "km/h", "ucum:km/h"));
		unitMapping.put("celsius degree", unitMapping.get("C"));
		unitMapping.put("m/s", new UnitOfMeasurement("m/s", "m/s", "ucum:m/s"));
		unitMapping.put("mg/m3", new UnitOfMeasurement("mg/m3", "mg/m3", "ucum:mg/m3"));
		unitMapping.put("degree", new UnitOfMeasurement("DegreesOfArc", "deg", "ucum:deg"));
		unitMapping.put("mW/cm2", new UnitOfMeasurement("milliwat per square cm", "mW/cm2", "ucum:mW/cm2"));
		unitMapping.put("%", new UnitOfMeasurement("percent", "%", "ucum:%"));
		unitMapping.put("hPa", unitMapping.get("mbar"));
		unitMapping.put("mm", unitMapping.get("mm/h"));

		addMultiDsOpList(new int[]{3, 5, 4});
		addMultiDsOpList(new int[]{31, 32, 33});
		addMultiDsOpList(new int[]{11, 10});
	}

	private void addMultiDsOpList(int[] list) {
		for (Integer prodId : list) {
			multiDsOps.put(prodId, list);
		}
	}

	private UnitOfMeasurement getUnitFor(String external) {
		if (unitMapping == null) {
			initUnitMapping();
		}
		return unitMapping.get(external);
	}

	private class ObsListIter extends AbstractIterator<List<Observation>> {

		private boolean initialised = false;
		private Connection connection;

		public ObsListIter() throws SQLException, ServiceFailureException, URISyntaxException, IOException {
			initDatabse();
		}

		private void initDatabse() throws SQLException, ServiceFailureException, URISyntaxException, IOException {
			if (initialised) {
				return;
			}
			initialised = true;
			initUnitMapping();
			try {
				Class.forName(editorDbDriver.getValue());
			} catch (ClassNotFoundException ex) {
				LOGGER.error("Could not initialise database.", ex);
				throw new IllegalArgumentException(ex);
			}

			connection = DriverManager.getConnection(
					editorDbUrl.getValue(),
					editorDbUsername.getValue(),
					editorDbPassword.getValue());

			if (editorFullImport.getValue()) {
				unknownSensor = SensorThingsUtils.findOrCreateSensor(service, "unknown", "An unknown sensor type.");
				generateObservedProperties();
				generateStations();
				generateDatastreams();
			}
		}

		private void generateObservedProperties() throws SQLException, ServiceFailureException, URISyntaxException {
			String queryString = "select * from products;";
			PreparedStatement statement = connection.prepareStatement(queryString);
			ResultSet resultSet = statement.executeQuery();
			while (resultSet.next()) {
				int id = resultSet.getInt("id");
				if (ignoreProducts.contains(id)) {
					continue;
				}
				String name = resultSet.getString("product");
				String unit = resultSet.getString("measurement_unit");
				ObservedProperty observedProperty = findObservedProperty(id);
				if (observedProperty == null) {
					generateObservedProperty(id, name, unit);
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
					Object idsObject = props.get("sistemaIds");
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
			ObservedProperty op = new ObservedProperty(name, new URI("http://sistema.org/unknown"), name + " in " + unit);
			op.setDescription(name);
			Map<String, Object> props = new HashMap<>();
			props.put("sistemaId", id);
			props.put("sistemaIds", Arrays.asList(id));
			props.put("sistemaUnit", unit);
			op.setProperties(props);
			LOGGER.info("Creating observed property {}", name);
			service.create(op);
			observedProperties.put(id, op);
		}

		private void generateStations() throws SQLException, ServiceFailureException {
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
			props.put("sistemaId", id);
			thing.setProperties(props);

			if (noAct) {
				LOGGER.warn("Not creating {}", thing);
			} else {
				service.create(thing);
				stations.put(id, thing);
				LOGGER.info("Created station {}.", thing);
			}
		}

		private void generateDatastreams() throws SQLException, ServiceFailureException, IOException {
			PreparedStatement statement = connection.prepareStatement(CROSS_PRODUCT_QUERY);
			ResultSet resultSet = statement.executeQuery();
			while (resultSet.next()) {
				int observed_field = resultSet.getInt("observed_field");
				int stations_id = resultSet.getInt("stations_id");
				findDsMds(stations_id, observed_field);
			}
		}

		private Entity findDsMds(int stationId, int propertyId) throws ServiceFailureException, IOException {
			if (multiDsOps.containsKey(propertyId)) {
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
				datastream = generateMultiDatastreamFor(stationId, multiDsOps.get(propertyId));
				props.put(propertyId, datastream);
			}
			return datastream;
		}

		private MultiDatastream generateMultiDatastreamFor(int stationId, int... propertyIds) throws ServiceFailureException, IOException {
			Thing station = stations.get(stationId);
			if (station == null) {
				throw new IllegalStateException("Unknown station");
			}
			List<ObservedProperty> obsProps = new ArrayList<>();
			List<UnitOfMeasurement> uoms = new ArrayList<>();
			StringBuilder sistemaProductsBuilder = new StringBuilder();
			for (int propertyId : propertyIds) {
				ObservedProperty observedProperty = observedProperties.get(propertyId);
				if (observedProperty == null) {
					throw new IllegalStateException("Unknown obsProp");
				}
				obsProps.add(observedProperty);
				sistemaProductsBuilder.append("-").append(propertyId);
				uoms.add(getUnitFor(observedProperty.getProperties().get("sistemaUnit").toString()));
			}
			String sistemaProducts = sistemaProductsBuilder.substring(1);
			MultiDatastream datastream = station.multiDatastreams()
					.query()
					.filter("properties/sistemaProducts eq '" + sistemaProducts + "'")
					.first();
			if (datastream != null) {
				return datastream;
			}
			String dsName = obsProps.get(0).getName() + " at " + station.getName();
			if (noAct) {
				LOGGER.warn("Not creating datastream {}", dsName);
				return null;
			}
			Map<String, Object> properties = new HashMap<>();
			properties.put("sistemaProducts", sistemaProducts);
			datastream = SensorThingsUtils.findOrCreateMultiDatastream(
					service,
					dsName,
					dsName,
					uoms,
					station,
					obsProps,
					unknownSensor,
					properties,
					SensorThingsUtils.AggregationLevels.DAILY);
			return datastream;
		}

		private Datastream generateDatastreamFor(int stationId, int propertyId) throws ServiceFailureException, IOException {
			Thing station = stations.get(stationId);
			ObservedProperty observedProperty = observedProperties.get(propertyId);
			if (station == null || observedProperty == null) {
				throw new IllegalStateException("Unknown station or obsProp");
			}
			String unit = observedProperty.getProperties().get("sistemaUnit").toString();
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
			properties.put("sistemaProducts", propertyId);
			datastream = SensorThingsUtils.findOrCreateDatastream(
					service,
					dsName,
					dsName,
					properties,
					station,
					observedProperty,
					getUnitFor(unit),
					unknownSensor,
					SensorThingsUtils.AggregationLevels.DAILY);
			return datastream;
		}

		@Override
		protected List<Observation> computeNext() {

			return endOfData();
		}

	}
}
