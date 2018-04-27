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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.importers;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.AbstractIterator;
import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Importer;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.JsonUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Location;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.TimeObject;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.query.Query;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.geojson.GeoJsonObject;
import org.geojson.LngLatAlt;
import org.geojson.Point;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

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
public class ImporterFmiForecasts implements Importer {

	/**
	 * The logger for this class.
	 */
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ImporterFmiForecasts.class);
	private static final String NUMBER_SPLIT_REGEX = "[^-+0-9.]+";
	private static final String POSITION_REGEX = "([-+]?[0-9]+(\\.[0-9]*)?)";
	private static final Pattern POSITION_PATTERN = Pattern.compile(POSITION_REGEX);

	private EditorMap<Map<String, Object>> editor;
	private EditorInt editorSleep;
	private EditorString editorSettingsThingFilter;
	private EditorString editorTranslator;

	private SensorThingsService service;
	private Translator translator;

	private boolean verbose;
	private boolean noAct = false;

	private long sleepTime;

	@Override
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	@Override
	public void setNoAct(boolean noAct) {
		this.noAct = noAct;
	}

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx) {
		service = context;
		getConfigEditor(context, edtCtx).setConfig(config);
		sleepTime = 1000 * editorSleep.getValue();

		try {
			translator = new Translator(editorTranslator.getValue());
		} catch (IOException exc) {
			throw new IllegalArgumentException(exc);
		}
	}

	@Override
	public ConfigEditor<?> getConfigEditor(SensorThingsService context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorMap<>();

			editorSettingsThingFilter = new EditorString("name eq 'FMI Settings'", 1, "SettingsThing", "The filter to find the Thing storing the settings.");
			editor.addOption("settingsThingFilter", editorSettingsThingFilter, false);

			editorTranslator = new EditorString("{\"from\":\"to\"}", 10, "Translations", "A map that translates input values to url values.");
			editor.addOption("translations", editorTranslator, true);

			editorSleep = new EditorInt(0, 99999, 1, 1, "Sleep", "The number of seconds to sleep after every imported run-section.");
			editor.addOption("sleepTime", editorSleep, true);
		}
		return editor;
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

	private static String fetchFromUrl(String targetUrl) throws ImportException {
		try {
			LOGGER.debug("Fetching: {}", targetUrl);
			CloseableHttpClient client = HttpClients.createSystem();
			HttpGet get = new HttpGet(targetUrl);
			CloseableHttpResponse response = client.execute(get);
			String data = EntityUtils.toString(response.getEntity(), Charset.forName("UTF-8"));
			return data;
		} catch (IOException ex) {
			LOGGER.error("Failed to fetch url " + targetUrl, ex);
			throw new ImportException("Failed to fetch url " + targetUrl, ex);
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

	private class ObsListIter extends AbstractIterator<List<Observation>> {

		private String dataUrl;
		private Iterator<Thing> stations;

		public ObsListIter() throws ImportException, URISyntaxException {
			try {
				Thing settingsThing = service.things().query().filter(editorSettingsThingFilter.getValue()).first();
				dataUrl = settingsThing.getProperties().get("dataUrl").toString();
				stations = generateStationIterator();
			} catch (ServiceFailureException exc) {
				LOGGER.error("Failed.", exc);
				throw new RuntimeException(exc);
			}
		}

		private Iterator<Thing> generateStationIterator() throws ServiceFailureException {

			Query<Thing> stationQuery = service.things().query()
					.filter("properties/type eq 'station'"
							+ " and properties/awaaId gt 0")
					.select("id", "name", "description", "properties")
					.top(1000)
					.expand("Locations($select=encodingType,location)"
							+ ",Datastreams($select=id,name,unitOfMeasurement;$filter=length(ObservedProperty/properties/fmiId) gt 0;$expand=ObservedProperty($select=id,name,properties))");
			EntityList<Thing> stationList = stationQuery.list();
			LOGGER.info("Stations: {}", stationList.size());
			return stationList.fullIterator();
		}

		private JsonNode fetchList(String rawUrl, String walkPath) throws ImportException {
			String replacedUrl = translator.replaceIn(rawUrl, true);
			String baseJson = fetchFromUrl(replacedUrl);
			JsonNode riversNode = convertJson(baseJson);
			JsonNode listJson = JsonUtils.walk(riversNode, walkPath);
			if (!listJson.isArray()) {
				throw new ImportException("Rows path did not lead to an array.");
			}
			return listJson;
		}

		private Document fetchXmlDocument(String rawUrl) throws ImportException {
			try {
				String replacedUrl = translator.replaceIn(rawUrl, true);
				String baseXml = fetchFromUrl(replacedUrl);
				DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
				dbf.setNamespaceAware(true);
				DocumentBuilder builder = dbf.newDocumentBuilder();
				Document document = builder.parse(new InputSource(new StringReader(baseXml)));
				return document;
			} catch (SAXException | IOException | ParserConfigurationException ex) {
				LOGGER.error("Failed to fetch and parse url " + rawUrl, ex);
				throw new ImportException("Failed to fetch and parse url " + rawUrl, ex);
			}
		}

		private List<Observation> computeForStation() throws IOException, ServiceFailureException, ImportException, XPathExpressionException {
			Thing station = stations.next();
			Location location = station.getLocations().toList().get(0);
			if (location == null) {
				LOGGER.warn("No location for station {} ({}).", station.getId(), station.getName());
				return Collections.emptyList();
			}
			GeoJsonObject geoObj = location.getLocation();
			String lat, lon;
			if (geoObj instanceof Point) {
				Point point = (Point) geoObj;
				LngLatAlt coordinates = point.getCoordinates();
				lat = Double.toString(coordinates.getLatitude());
				lon = Double.toString(coordinates.getLongitude());
				translator.put("latitude", lat);
				translator.put("longitude", lon);
			}

			Document xmlDoc = fetchXmlDocument(dataUrl);

			// Find the datarecord definition
			XPath xpath = XPathFactory.newInstance().newXPath();
			NamespaceContextMap nsCtx = new NamespaceContextMap();
			nsCtx.addPrefix("wfs", "http://www.opengis.net/wfs/2.0");
			nsCtx.addPrefix("omso", "http://inspire.ec.europa.eu/schemas/omso/3.0");
			nsCtx.addPrefix("om", "http://www.opengis.net/om/2.0");
			nsCtx.addPrefix("gmlcov", "http://www.opengis.net/gmlcov/1.0");
			nsCtx.addPrefix("swe", "http://www.opengis.net/swe/2.0");
			nsCtx.addPrefix("gml", "http://www.opengis.net/gml/3.2");
			xpath.setNamespaceContext(nsCtx);

			List<String> xmlFieldNames = new ArrayList<>();
			int propertyCount;
			{
				String expression = "/wfs:FeatureCollection/wfs:member/omso:GridSeriesObservation/om:result/gmlcov:MultiPointCoverage/gmlcov:rangeType/swe:DataRecord/swe:field/@name";
				NodeList fieldList = (NodeList) xpath.evaluate(expression, xmlDoc, XPathConstants.NODESET);
				LOGGER.info("Found {} for {}", fieldList.getLength(), expression);
				for (int i = 0; i < fieldList.getLength(); i++) {
					Node fieldNode = fieldList.item(i);
					xmlFieldNames.add(fieldNode.getNodeValue());
				}
				propertyCount = xmlFieldNames.size();
				LOGGER.info("Found {} observed properties", propertyCount);
			}

			List<Long> times = new ArrayList<>();
			{
				String dimExpression = "/wfs:FeatureCollection/wfs:member/omso:GridSeriesObservation/om:result/gmlcov:MultiPointCoverage/gml:domainSet/gmlcov:SimpleMultiPoint/@srsDimension";
				int dimension = Integer.parseInt(xpath.evaluate(dimExpression, xmlDoc));
				LOGGER.info("Position has {} dimensions", dimension);
				if (dimension != 3) {
					throw new IllegalStateException("Expected 3 dimensions, got " + dimension);
				}
				String positionExpresson = "/wfs:FeatureCollection/wfs:member/omso:GridSeriesObservation/om:result/gmlcov:MultiPointCoverage/gml:domainSet/gmlcov:SimpleMultiPoint/gmlcov:positions";
				String positionString = xpath.evaluate(positionExpresson, xmlDoc);
				String[] split = positionString.trim().split(NUMBER_SPLIT_REGEX);
				int i = 0;
				for (String valueString : split) {
					i++;
					if (i == 3) {
						Long time = Long.parseLong(valueString);
						times.add(time);
						i = 0;
					}
				}
				if (i != 0) {
					LOGGER.error("Found incorrect number of positions. Have {} left over.", i);
				}
				LOGGER.info("Found {} times", times.size());
			}

			Map<Integer, Datastream> datastreams = new HashMap<>();
			{
				for (Iterator<Datastream> it = station.getDatastreams().fullIterator(); it.hasNext();) {
					Datastream datastream = it.next();
					String fmiProperty = datastream.getObservedProperty().getProperties().get("fmiId").toString();
					int propIndex = xmlFieldNames.indexOf(fmiProperty);
					if (propIndex < 0) {
						LOGGER.debug("No property named {} for datastream {}.", fmiProperty, datastream.getName());
					} else {
						datastreams.put(propIndex, datastream);
					}
				}
			}

			ZonedDateTime resultTime;
			{
				String resultTimeExpression = "/wfs:FeatureCollection/wfs:member/omso:GridSeriesObservation/om:resultTime/gml:TimeInstant/gml:timePosition";
				String dataString = xpath.evaluate(resultTimeExpression, xmlDoc);
				resultTime = ZonedDateTime.ofInstant(Instant.parse(dataString), ZoneOffset.UTC);
			}

			List<Observation> observations = new ArrayList<>();
			{
				String dataExpression = "/wfs:FeatureCollection/wfs:member/omso:GridSeriesObservation/om:result/gmlcov:MultiPointCoverage/gml:rangeSet/gml:DataBlock/gml:doubleOrNilReasonTupleList";
				String dataString = xpath.evaluate(dataExpression, xmlDoc);
				String[] split = dataString.trim().split(NUMBER_SPLIT_REGEX);
				int i = 0;
				int timeIdx = 0;
				ZonedDateTime dateTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(times.get(timeIdx)), ZoneOffset.UTC);
				boolean outOfTimes = false;
				for (String valueString : split) {
					if (outOfTimes) {
						LOGGER.warn("Data found after last time instant was used up!");
					}
					Datastream datastream = datastreams.get(i);
					if (datastream != null) {
						BigDecimal value = new BigDecimal(valueString);
						LOGGER.info("Date {}, value {}, datastream {}", dateTime, value, datastream.getName());
						Observation o = new Observation(value, datastream);
						o.setPhenomenonTime(new TimeObject(dateTime));
						o.setResultTime(resultTime);
						observations.add(o);
					}
					i++;
					if (i == propertyCount) {
						i = 0;
						timeIdx++;
						if (timeIdx < times.size()) {
							dateTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(times.get(timeIdx)), ZoneOffset.UTC);
						} else {
							outOfTimes = true;
						}
					}
				}
				if (i != 0) {
					LOGGER.error("Found incorrect number of data items. Have {} left over.", i);
				}

			}

			return observations;
		}

		@Override
		protected List<Observation> computeNext() {
			while (stations.hasNext()) {
				try {
					return computeForStation();
				} catch (Exception exc) {
					LOGGER.error("Failed", exc);
				}
			}
			return endOfData();
		}
	}

	private static final class NamespaceContextMap implements NamespaceContext {

		private final Map<String, String> prefixMap;
		private final Map<String, Set<String>> namespaceMap;

		public NamespaceContextMap() {
			prefixMap = new HashMap<>();
			namespaceMap = new HashMap<>();
			addPrefix(XMLConstants.XML_NS_PREFIX, XMLConstants.XML_NS_URI);
			addPrefix(XMLConstants.XMLNS_ATTRIBUTE, XMLConstants.XMLNS_ATTRIBUTE_NS_URI);
		}

		public void addPrefix(String prefix, String uri) {
			prefixMap.put(prefix, uri);
			LOGGER.debug("{} -> {}", prefix, uri);
			Set<String> prefixes = namespaceMap.get(uri);
			if (prefixes == null) {
				prefixes = new HashSet<>();
				namespaceMap.put(uri, prefixes);
			}
			prefixes.add(prefix);
		}

		@Override
		public String getNamespaceURI(String prefix) {
			checkNotNull(prefix);
			String nsURI = prefixMap.get(prefix);
			if (nsURI == null) {
				LOGGER.warn("Not found: {}", prefix);
				return XMLConstants.NULL_NS_URI;
			}
			return nsURI;
		}

		@Override
		public String getPrefix(String namespaceURI) {
			checkNotNull(namespaceURI);
			Set<String> set = namespaceMap.get(namespaceURI);
			if (set == null) {
				LOGGER.warn("Not found: {}", namespaceURI);
				return null;
			}
			return set.iterator().next();
		}

		@Override
		public Iterator<String> getPrefixes(String namespaceURI) {
			LOGGER.info("Finding all: {}", namespaceURI);
			checkNotNull(namespaceURI);
			Set<String> set = namespaceMap.get(namespaceURI);
			return set.iterator();
		}

		private void checkNotNull(String value) {
			if (value == null) {
				throw new IllegalArgumentException("null not allowed");
			}
		}
	}
}
