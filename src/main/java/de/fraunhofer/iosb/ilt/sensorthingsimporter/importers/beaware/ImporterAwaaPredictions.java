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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.beaware;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.AbstractConfigurable;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.ConfigurationException;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Importer;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.JsonUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.SensorThingsUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.SensorThingsUtils.AggregationLevels;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.SensorThingsUtils.addAllToMap;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UrlUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.parsers.ParserNumber;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.parsers.ParserZonedDateTime;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
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
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.collections4.comparators.ComparatorChain;
import org.geojson.LineString;
import org.geojson.LngLatAlt;
import org.geojson.Point;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

/**
 * <pre>
 * Runs (WS_Runs.php)
 * -> Rivers for a Run (WS_Rivers.php?idrunmike=1080)
 * -> Sections for a Run & River (WS_RiverSections.php?idrunmike=1080&idriver=6)
 * -> Predictions for a Run & River & Section (WS_SectionLevels.php?idrunmike=1050&idriver=6&idsection=495)
 * River (Thing; properties/awaaId)
 * Section (Thing; properties/riverId properties/awaaId)
 * Prediction (Observation; parameters/runId)
 * </pre>
 *
 * @author scf
 */
public class ImporterAwaaPredictions extends AbstractConfigurable<SensorThingsService, Object> implements Importer {

	/**
	 * The logger for this class.
	 */
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ImporterAwaaPredictions.class);

	@ConfigurableField(
			label = "Full Import", description = "Import all rivers and sections every time.",
			optional = true,
			editor = EditorBoolean.class)
	@EditorBoolean.EdOptsBool(dflt = true)
	private boolean fullImport;

	@ConfigurableField(label = "Runs Index", description = "The url to fetch the runs list from.", editor = EditorString.class)
	@EditorString.EdOptsString()
	private String runsUrl;

	@ConfigurableField(label = "Rivers Index", description = "The url to fetch the rivers from.", editor = EditorString.class)
	@EditorString.EdOptsString()
	private String riversUrl;

	@ConfigurableField(label = "Sections Index", description = "The url to fetch the river sections from.", editor = EditorString.class)
	@EditorString.EdOptsString()
	private String sectionsUrl;

	@ConfigurableField(label = "Levels Index", description = "The url to fetch the water levels from.", editor = EditorString.class)
	@EditorString.EdOptsString()
	private String levelsUrl;

	@ConfigurableField(
			label = "Translations", description = "A map that translates input values to url values.",
			editor = EditorString.class)
	@EditorString.EdOptsString(dflt = "{\"from\":\"to\"}", lines = 5)
	private String translations;

	@ConfigurableField(
			label = "Time Parser", description = "The parser that converts times.",
			editor = EditorSubclass.class)
	@EditorSubclass.EdOptsSubclass(iface = ParserZonedDateTime.class)
	private ParserZonedDateTime timeParser;

	@ConfigurableField(
			label = "Sleep", description = "The number of seconds to sleep after every imported run-section.",
			editor = EditorInt.class, optional = true)
	@EditorInt.EdOptsInt(dflt = 1, min = 0, max = 99999, step = 1)
	private int sleepTime;

	@ConfigurableField(
			label = "Validate # Runs",
			description = "If non-zero, instead of importing new data, the last # imported runs are re-imported.\n"
			+ "Make sure you use a Validator with this option.",
			editor = EditorInt.class, optional = true)
	@EditorInt.EdOptsInt(dflt = 0, min = 0, max = 99999, step = 1)
	private int runsToValidate;

	private Translator translator;
	private SensorThingsService service;
	private ParserNumber numberParser = new ParserNumber();

	private boolean verbose;
	private boolean noAct = false;

	@Override
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	@Override
	public void setNoAct(boolean noAct) {
		this.noAct = noAct;
	}

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx, ConfigEditor<?> configEditor) throws ConfigurationException {
		service = context;
		super.configure(config, context, edtCtx, configEditor);
		translator = new Translator();
		translator.setMappings(translations);
	}

	@Override
	public Iterator<List<Observation>> iterator() {
		try {
			ObsListIter obsListIter = new ObsListIter();
			return obsListIter;
		} catch (ImportException | URISyntaxException exc) {
			LOGGER.error("Failed", exc);
			throw new IllegalStateException("Failed to handle csv file.", exc);
		}
	}

	private static class Run {

		public final int id;
		public final ZonedDateTime start;
		public final ZonedDateTime end;

		public Run(int id, ZonedDateTime start, ZonedDateTime end) {
			this.id = id;
			this.start = start;
			this.end = end;
		}

	}

	private static class River {

		public final int id;
		public final String name;
		public Thing thing;

		public River(int id, String name) {
			this.id = id;
			this.name = name;
		}

		public River(int id, String name, Thing thing) {
			this.id = id;
			this.name = name;
			this.thing = thing;
		}

		@Override
		public String toString() {
			return name + " (" + id + ")";
		}
	}

	private static class RiverSection {

		public final int id;
		public final String name;
		public final River river;
		public double lat;
		public double lon;
		public double distance;
		public List<BigDecimal> thresholds = new ArrayList<>();
		public Thing thing;

		public RiverSection(int id, String name, River river) {
			this.id = id;
			this.name = name;
			this.river = river;
		}

		public RiverSection(int id, String name, River river, Thing thing) {
			this.id = id;
			this.name = name;
			this.river = river;
			this.thing = thing;
		}

		@Override
		public String toString() {
			return name + " (" + id + ", " + river.id + ")";
		}

	}

	private static JsonNode convertJson(String json) throws ImportException {
		try {
			ObjectMapper mapper = new ObjectMapper();
			mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
			JsonNode jsonNode = mapper.readTree(json);
			return jsonNode;
		} catch (IOException ex) {
			LOGGER.error("Failed to convert to json.", ex);
			throw new ImportException("Failed to convert to json", ex);
		}
	}

	private class ObsListIter implements Iterator<List<Observation>> {

		private final SortedMap<Integer, Run> runs;
		private final List<Integer> runIds = new ArrayList<>();
		private ObservedProperty opWaterLevel;
		private Sensor sensorWaterModel;
		private Iterator<Datastream> datastreams;
		private final Iterator<MultiDatastream> multiDatastreams;
		private boolean hasMore;

		public ObsListIter() throws ImportException, URISyntaxException {
			runs = initRuns();
			runIds.addAll(runs.keySet());
			try {
				// Figure out which runs need importing.
				if (fullImport) {
					Map<String, Object> opProps = new HashMap<>();
					opProps.put("awaaId", 6);
					opWaterLevel = SensorThingsUtils.findOrCreateOp(
							service,
							"Water Level",
							new URI("http://dbpedia.org/page/Water_level"),
							"The water level in a river.",
							opProps,
							"properties/awaaId eq 6",
							true);
					if (opWaterLevel == null) {
						throw new ImportException("No ObservedProperty for water level (awaaId=6)");
					}
					LOGGER.info("OP WaterLevel: {}", opWaterLevel.getId());

					sensorWaterModel = service.sensors().query()
							.filter("properties/awaaId eq 1")
							.first();
					if (sensorWaterModel == null) {
						sensorWaterModel = new Sensor("WaterLevelPredictor", "Model that generates water level forecasts", "text/plain", "No details");
						Map<String, Object> properties = new HashMap<>();
						properties.put("awaaId", 1);
						sensorWaterModel.setProperties(properties);
						service.create(sensorWaterModel);
						LOGGER.info("Created model {}.", sensorWaterModel.getId());
					}
					LOGGER.info("Model: {}", sensorWaterModel.getId());

					Run run = runs.get(runs.lastKey());
					SortedMap<Integer, River> rivers = importRivers(run);
					for (River river : rivers.values()) {
						SortedMap<Integer, RiverSection> sections = importRiverSections(run, river);
						checkLocationOfRiver(river, sections);
					}
				}
			} catch (ServiceFailureException exc) {
				LOGGER.error("Failed.", exc);
				throw new ImportException(exc);
			}

			datastreams = generateDatastreamIterator();
			multiDatastreams = null;
		}

		private Iterator<Datastream> generateDatastreamIterator() {
			try {
				Query<Datastream> dsQuery = service.datastreams().query()
						.filter("properties/type eq 'forecast'"
								+ " and Thing/properties/awaaId gt 0"
								+ " and Thing/properties/type eq 'riverSection'"
								+ " and ObservedProperty/properties/awaaId gt 0"
								+ " and not endswith(name, ']')")
						.top(1000);
				if (runsToValidate == 0) {
					dsQuery.expand("ObservedProperty($select=id,name,properties),"
							+ "Thing($select=id,name,properties),"
							+ "Observations($orderby=phenomenonTime desc;$top=1;$select=result,phenomenonTime,parameters)");
				} else {
					dsQuery.expand("ObservedProperty($select=id,name,properties),"
							+ "Thing($select=id,name,properties)");
				}
				EntityList<Datastream> dsList = dsQuery.list();
				LOGGER.info("Datastreams: {}", dsList.size());
				hasMore = false;
				return dsList.fullIterator();
			} catch (ServiceFailureException exc) {
				LOGGER.error("Failed.", exc);
				throw new RuntimeException(exc);
			}
		}

		private SortedMap<Integer, Run> initRuns() throws ImportException {
			String runsUrlRaw = runsUrl;
			String runsUrl = translator.replaceIn(runsUrlRaw, true);
			String runsJson = UrlUtils.fetchFromUrl(runsUrl);
			JsonNode runsNode = convertJson(runsJson);
			JsonNode listJson = JsonUtils.walk(runsNode, "rows");
			if (!listJson.isArray()) {
				throw new ImportException("Rows path did not lead to an array.");
			}
			SortedMap<Integer, Run> runMap = new TreeMap<>();
			for (JsonNode element : listJson) {
				Integer runId = Integer.valueOf(JsonUtils.walk(element, "ID").asText());
				ZonedDateTime from = timeParser.parse(JsonUtils.walk(element, "DAL"));
				ZonedDateTime to = timeParser.parse(JsonUtils.walk(element, "AL"));
				Run run = new Run(runId, from, to);
				runMap.put(runId, run);
			}
			LOGGER.info("Found {} runs", runMap.size());
			return runMap;
		}

		private JsonNode fetchList(String rawUrl, String walkPath) throws ImportException {
			String replacedUrl = translator.replaceIn(rawUrl, true);
			String baseJson = UrlUtils.fetchFromUrl(replacedUrl);
			JsonNode riversNode = convertJson(baseJson);
			JsonNode listJson = JsonUtils.walk(riversNode, walkPath);
			if (!listJson.isArray()) {
				throw new ImportException("Rows path did not lead to an array.");
			}
			return listJson;
		}

		/**
		 * Assumes the river id does not change between runs.
		 */
		private SortedMap<Integer, River> importRivers(Run run) throws ImportException {
			translator.put("idrunmike", Integer.toString(run.id));
			JsonNode listJson = fetchList(riversUrl, "rows");
			SortedMap<Integer, River> riverMap = new TreeMap<>();
			for (JsonNode element : listJson) {
				Integer riverId = Integer.valueOf(JsonUtils.walk(element, "ID").asText());
				String name = JsonUtils.walk(element, "DESCR").asText();
				River river = new River(riverId, name);
				riverMap.put(riverId, river);
			}
			for (River river : riverMap.values()) {
				try {
					getOrCreateThingFor(river);
				} catch (ServiceFailureException ex) {
					LOGGER.error("Failed to get or create Thing for River " + river.name);
				}
			}
			LOGGER.info("Found {} rivers", riverMap.size());
			return riverMap;
		}

		private SortedMap<Integer, RiverSection> importRiverSections(Run run, River river) throws ImportException {
			if (river.thing == null || river.thing.getId() == null) {
				throw new ImportException("River without a Thing, for " + river);
			}
			translator.put("idrunmike", Integer.toString(run.id));
			translator.put("idriver", Integer.toString(river.id));
			JsonNode listJson = fetchList(sectionsUrl, "rows");
			SortedMap<Integer, RiverSection> sectionMap = new TreeMap<>();
			for (JsonNode element : listJson) {
				Integer sectionId = Integer.valueOf(JsonUtils.walk(element, "IDSEZIONE").asText());
				Integer riverId = Integer.valueOf(JsonUtils.walk(element, "IDFIUME").asText());
				if (riverId != river.id) {
					LOGGER.error("Section {} has river id {}, should be {}.", sectionId, riverId, river.id);
					throw new ImportException("Section has incorrect riverId");
				}
				String name = JsonUtils.walk(element, "DESCR").asText();
				RiverSection section = new RiverSection(sectionId, name, river);
				sectionMap.put(sectionId, section);
				section.distance = Double.valueOf(JsonUtils.walk(element, "KM").asText());
				section.lat = Double.valueOf(JsonUtils.walk(element, "LAT").asText());
				section.lon = Double.valueOf(JsonUtils.walk(element, "LON").asText());
				section.thresholds.add(new BigDecimal(JsonUtils.walk(element, "ALLARME1").asText()));
				section.thresholds.add(new BigDecimal(JsonUtils.walk(element, "ALLARME2").asText()));
				section.thresholds.add(new BigDecimal(JsonUtils.walk(element, "ALLARME3").asText()));
			}
			for (RiverSection section : sectionMap.values()) {
				try {
					getOrCreateThingFor(section);
				} catch (ServiceFailureException | IOException ex) {
					LOGGER.error("Failed to get or create Thing for River Section " + section.name);
				}
			}
			LOGGER.info("Found {} river sections", sectionMap.size());
			return sectionMap;
		}

		private List<Observation> importObservations(Datastream ds, int runId, int riverId, int sectionId) throws ImportException {
			translator.put("idrunmike", Integer.toString(runId));
			translator.put("idriver", Integer.toString(riverId));
			translator.put("idsection", Integer.toString(sectionId));
			Run run = runs.get(runId);
			Interval validTime = Interval.of(run.start.toInstant(), run.end.toInstant());
			Map<String, Object> parameters = new HashMap<>();
			parameters.put("runId", run.id);

			List<Observation> observations = new ArrayList<>();
			JsonNode listJson = fetchList(levelsUrl, "rows");
			for (JsonNode element : listJson) {
				if (sectionId != Integer.valueOf(JsonUtils.walk(element, "IDSEZIONE").asText())) {
					throw new ImportException("Incorrect section id in response.");
				}
				if (riverId != Integer.valueOf(JsonUtils.walk(element, "IDFIUME").asText())) {
					throw new ImportException("Incorrect river id in response.");
				}
				ZonedDateTime date = timeParser.parse(JsonUtils.walk(element, "DATAORA"));
				Number level = numberParser.parse(JsonUtils.walk(element, "LIVELLO"));
				Observation observation = new Observation(level, ds);
				observation.setPhenomenonTimeFrom(date);
				observation.setValidTime(validTime);
				observation.setParameters(parameters);
				observations.add(observation);
			}
			return observations;
		}

		private void getOrCreateThingFor(River river) throws ServiceFailureException {
			if (river.thing != null) {
				return;
			}
			Query<Thing> tQuery = service.things().query()
					.filter("properties/type eq 'river' and properties/awaaId eq " + river.id);
			EntityList<Thing> tList = tQuery.list();
			if (tList.size() > 1) {
				LOGGER.error("Found multiple rivers with id " + river.id);
			}
			if (tList.size() > 0) {
				river.thing = tList.toList().get(0);
				return;
			}
			Map<String, Object> properties = new HashMap<>();
			properties.put("type", "river");
			properties.put("awaaId", river.id);
			Thing thing = new Thing(river.name, "River " + river.name, properties);
			service.create(thing);
			river.thing = thing;
			LOGGER.info("Created river {}: {} {}.", river.id, river.thing.getId(), river.name);
		}

		private void getOrCreateThingFor(RiverSection section) throws ServiceFailureException, IOException {
			if (section.thing != null) {
				return;
			}
			Query<Thing> tQuery = service.things().query()
					.filter("properties/type eq 'riverSection' and properties/awaaId eq " + section.id + " and properties/riverAwaaId eq " + section.river.id)
					.expand("Datastreams");
			EntityList<Thing> tList = tQuery.list();
			if (tList.size() > 1) {
				LOGGER.error("Found multiple river sections with id " + section.id);
			}
			Thing thing;
			Map<String, Object> thingProps = new HashMap<>();
			thingProps.put("type", "riverSection");
			thingProps.put("awaaId", section.id);
			thingProps.put("riverAwaaId", section.river.id);
			thingProps.put("distance", BigDecimal.valueOf(section.distance));
			// Old threshold locations, should be removed in the future.
			thingProps.put("treshold1", section.thresholds.get(0));
			thingProps.put("treshold2", section.thresholds.get(1));
			thingProps.put("treshold3", section.thresholds.get(2));

			if (tList.size() > 0) {
				section.thing = tList.toList().get(0);
				thing = section.thing;
				if (addAllToMap(thing.getProperties(), thingProps)) {
					service.update(thing);
				}
			} else {
				LOGGER.info("Creating section Thing {}.", section.name);
				thing = new Thing(section.name, "River section " + section.name, thingProps);
				service.create(thing);

				Location location = SensorThingsUtils.findOrCreateLocation(
						service,
						section.name,
						"Location of river section " + section.name,
						new Point(section.lon, section.lat));
				Thing tempThing = thing.withOnlyId();
				tempThing.getLocations().add(location);
				service.update(tempThing);
			}

			Map<String, Object> dsProps = new HashMap<>();
			dsProps.put("type", "forecast");
			dsProps.put("thresholds", section.thresholds);
			UnitOfMeasurement uom = new UnitOfMeasurement("metre", "m", "ucum:m");
			Datastream ds = SensorThingsUtils.findOrCreateDatastream(
					service,
					"Water level forecast river " + section.river.id + " section " + section.id,
					"Water level forecast river " + section.river.id + " section " + section.id,
					dsProps,
					thing,
					opWaterLevel,
					uom,
					sensorWaterModel,
					AggregationLevels.DAILY);

			section.thing = thing;
		}

		private void checkLocationOfRiver(River river, SortedMap<Integer, RiverSection> sections) throws ServiceFailureException {
			EntityList<Location> locations = river.thing.locations().query().list();
			if (locations.size() > 0) {
				return;
			}
			LOGGER.info("Creating Location for river {}.", river);
			List<RiverSection> sectionList = new ArrayList<>(sections.values());
			Collections.sort(
					sectionList,
					(o1, o2) -> new ComparatorChain<RiverSection>()
							.thenComparingDouble((value) -> {
								return value.distance;
							})
							.thenComparingInt((value) -> {
								return value.id;
							}).compare(o1, o2));
			List<LngLatAlt> points = new ArrayList<>();
			for (RiverSection section : sectionList) {
				points.add(new LngLatAlt(section.lon, section.lat));
			}
			LineString lineString = new org.geojson.LineString(points.toArray(new LngLatAlt[points.size()]));
			Location loc = new Location(
					"River " + river.name,
					"River " + river.name,
					"application/geo+json", lineString);
			loc.getThings().add(river.thing);
			service.create(loc);
		}

		private List<Observation> computeForDatastreams() throws IOException, ServiceFailureException, ImportException {
			if (runsToValidate == 0) {
				return computeForDatastreamsImport();
			} else {
				return computeForDatastreamsValidate();
			}
		}

		private List<Observation> computeForDatastreamsImport() throws IOException, ServiceFailureException, ImportException {
			Datastream ds = datastreams.next();
			int sectionAwaaId = JsonUtils.toInt(ds.getThing().getProperties().get("awaaId"));
			int riverAwaaId = JsonUtils.toInt(ds.getThing().getProperties().get("riverAwaaId"));

			int lastRunDs = JsonUtils.toInt(ds.getProperties().get("lastRunId"), 0);

			Observation first = ds.observations()
					.query()
					.orderBy("phenomenonTime desc,id desc")
					.first();
			int lastRunObs = JsonUtils.toInt(first.getParameters().get("runId"), lastRunDs);
			if (lastRunObs != lastRunDs) {
				LOGGER.warn("Datastream has different lastRunId ({}) than its Observations ({})", lastRunDs, lastRunObs);
			}
			int lastRun = lastRunDs;
			int nextRun = -1;
			for (Integer availableRun : runs.keySet()) {
				if (availableRun > lastRun) {
					nextRun = availableRun;
					hasMore = true;
					break;
				}
			}
			if (nextRun == -1) {
				// Nothing to do for this datastream.
				return Collections.EMPTY_LIST;
			}
			if (sleepTime > 0) {
				try {
					Thread.sleep(sleepTime * 1000);
				} catch (InterruptedException ex) {
					LOGGER.error("Rude wakeop!", ex);
				}
			}

			List<Observation> observations = importObservations(ds, nextRun, riverAwaaId, sectionAwaaId);
			LOGGER.info("Generated {} observations for river {}, section {}, run {}", observations.size(), riverAwaaId, sectionAwaaId, nextRun);

			if (!noAct) {
				// Update the property lastRunId, both in the local cache and on the server.
				Map<String, Object> properties = ds.getProperties();
				properties.put("lastRunId", nextRun);
				Datastream dsOnlyId = ds.withOnlyId();
				dsOnlyId.setProperties(properties);
				service.update(dsOnlyId);
			}
			return observations;
		}

		private List<Observation> computeForDatastreamsValidate() throws IOException, ServiceFailureException, ImportException {
			Datastream ds = datastreams.next();
			int sectionAwaaId = JsonUtils.toInt(ds.getThing().getProperties().get("awaaId"));
			int riverAwaaId = JsonUtils.toInt(ds.getThing().getProperties().get("riverAwaaId"));

			int lastRunDs = JsonUtils.toInt(ds.getProperties().get("lastRunId"), 0);

			List<Integer> validateRuns = null;
			for (int i = runIds.size() - 1; i >= 0; i--) {
				if (runIds.get(i) == lastRunDs) {
					validateRuns = runIds.subList(Math.max(0, i - runsToValidate + 1), i + 1);
					LOGGER.info("Validating runs {}", validateRuns);
					break;
				}
			}

			if (validateRuns == null) {
				// Nothing to do for this datastream.
				return Collections.EMPTY_LIST;
			}
			if (sleepTime > 0) {
				try {
					Thread.sleep(sleepTime * 1000);
				} catch (InterruptedException ex) {
					LOGGER.error("Rude wakeop!", ex);
				}
			}

			List<Observation> observations = new ArrayList<>();
			for (Integer runId : validateRuns) {
				observations.addAll(importObservations(ds, runId, riverAwaaId, sectionAwaaId));
			}
			LOGGER.info("Generated {} observations for river {}, section {}", observations.size(), riverAwaaId, sectionAwaaId);
			return observations;
		}

		private List<Observation> computeForMultiDatastreams() throws IOException, ServiceFailureException, ImportException {
			return Collections.EMPTY_LIST;
		}

		@Override
		public boolean hasNext() {
			return datastreams.hasNext() || (multiDatastreams != null && multiDatastreams.hasNext());
		}

		@Override
		public List<Observation> next() {
			while (datastreams.hasNext()) {
				try {
					return computeForDatastreams();
				} catch (Exception exc) {
					LOGGER.error("Failed", exc);
				}
			}
			if (hasMore) {
				datastreams = generateDatastreamIterator();
				return Collections.EMPTY_LIST;
			}
			while (multiDatastreams != null && multiDatastreams.hasNext()) {
				try {
					return computeForMultiDatastreams();
				} catch (Exception exc) {
					LOGGER.error("Failed", exc);
				}
			}
			return Collections.emptyList();
		}
	}

}
