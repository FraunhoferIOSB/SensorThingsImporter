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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.ConfigurationException;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Importer;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.timegen.TimeGen;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.parsers.document.DocumentParser;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Id;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.query.Query;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class ImporterAwaa implements Importer {

	/**
	 * The logger for this class.
	 */
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ImporterAwaa.class);

	private EditorMap<Map<String, Object>> editor;
	private EditorSubclass<SensorThingsService, Object, DocumentParser> editorDocumentParser;
	private EditorString editorFetchUrl;
	private EditorString editorDatastreamFilter;
	private EditorString editorMultiDatastreamFilter;
	private EditorString editorDatastreamExclude;
	private EditorString editorMultiDatastreamExclude;
	private EditorSubclass<SensorThingsService, Object, TimeGen> editorStartTime;
	private EditorString editorTranslator;
	private EditorString editorTimeFormat;
	private EditorString editorTimeZone;
	private EditorInt editorMaxHours;

	private SensorThingsService service;
	private DateTimeFormatter timeFormatter;
	private Translator translator;
	private TimeGen startTime;
	private DocumentParser docParser;
	private boolean verbose;
	private Set<Id> dsExclude = new HashSet<>();
	private Set<Id> mdsExclude = new HashSet<>();

	@Override
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	@Override
	public void setNoAct(boolean noAct) {
		// Nothing to set.
	}

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx, ConfigEditor<?> configEditor) throws ConfigurationException {
		service = context;
		getConfigEditor(context, edtCtx).setConfig(config);
		timeFormatter = DateTimeFormatter.ofPattern(editorTimeFormat.getValue());
		if (!editorTimeZone.getValue().isEmpty()) {
			timeFormatter = timeFormatter.withZone(ZoneId.of(editorTimeZone.getValue()));
		}
		try {
			translator = new Translator(editorTranslator.getValue());
		} catch (IOException exc) {
			throw new IllegalArgumentException(exc);
		}
		docParser = editorDocumentParser.getValue();
		startTime = editorStartTime.getValue();
	}

	@Override
	public ConfigEditor<?> getConfigEditor(SensorThingsService context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorMap<>();

			editorDocumentParser = new EditorSubclass<>(context, edtCtx, DocumentParser.class, "DocumentParser", "The parser that transforms a document into Observations.");
			editor.addOption("documentParser", editorDocumentParser, false);

			editorFetchUrl = new EditorString("http://someHost.com/some/path?station={awaaId}&timefrom={startTime}&timeto={endTime}&what={obsPropName}", 1, "fetchUrl", "The url to fetch data from.");
			editor.addOption("fetchUrl", editorFetchUrl, false);

			editorStartTime = new EditorSubclass(context, edtCtx, TimeGen.class, "Start time", "When to start");
			editor.addOption("startTime", editorStartTime, true);

			editorTranslator = new EditorString("{\"from\":\"to\"}", 10, "Translations", "A map that translates input values to url values.");
			editor.addOption("translations", editorTranslator, true);

			editorTimeFormat = new EditorString("yyyy-MM-dd HH:mm:ssX", 1, "Time Format", "How to format times in the url.");
			editor.addOption("timeFormat", editorTimeFormat, false);

			editorTimeZone = new EditorString("+01:00", 1, "Time Zone", "The time zone to convert times in the url to.");
			editor.addOption("timeZone", editorTimeZone, false);

			editorMaxHours = new EditorInt(1, 99999, 1, 24, "Max Hours", "The maximum number of hours to fetch observations for.");
			editor.addOption("maxHours", editorMaxHours, true);

			editorDatastreamFilter = new EditorString(
					"Thing/properties/type eq 'station' and Thing/properties/awaaId gt 0 and ObservedProperty/properties/awaaId gt 0 and not endswith(name, ']')",
					1, "Datastream Filter", "The filter to use in the query to find Datastreams.");
			editor.addOption("datastreamFilter", editorDatastreamFilter, false);

			editorDatastreamExclude = new EditorString(
					"",
					1, "Datastream Exclude", "Filter for Datastreams to explicitly exclude from the import.");
			editor.addOption("datastreamExclude", editorDatastreamExclude, true);

			editorMultiDatastreamFilter = new EditorString(
					"Thing/properties/type eq 'station' and Thing/properties/awaaId gt 0 and ObservedProperty/properties/awaaId gt 0 and not endswith(name, ']')",
					1, "MultiDatastream Filter", "The filter to use in the query to find MultiDatastreams.");
			editor.addOption("multiDatastreamFilter", editorMultiDatastreamFilter, false);

			editorMultiDatastreamExclude = new EditorString(
					"",
					1, "MultiDatastream Exclude", "Filter for MultiDatastreams to explicitly exclude from the import.");
			editor.addOption("multiDatastreamExclude", editorMultiDatastreamExclude, true);
		}
		return editor;

	}

	@Override
	public Iterator<List<Observation>> iterator() {
		try {
			ObsListIter obsListIter = new ObsListIter();
			return obsListIter;
		} catch (ImportException exc) {
			LOGGER.error("Failed", exc);
			throw new IllegalStateException("Failed to handle csv file.", exc);
		}
	}

	private class ObsListIter implements Iterator<List<Observation>> {

		private final Iterator<Datastream> datastreams;
		private final Iterator<MultiDatastream> multiDatastreams;

		public ObsListIter() throws ImportException {
			try {
				{
					Query<Datastream> dsQuery = service.datastreams().query()
							.filter(editorDatastreamFilter.getValue())
							.top(1000)
							.expand("ObservedProperty($select=id,name,properties),Thing($select=id,name,properties),Observations($orderby=phenomenonTime desc;$top=1;$select=result,phenomenonTime)");
					EntityList<Datastream> dsList = dsQuery.list();
					LOGGER.info("Datastrams: {}", dsList.size());
					datastreams = dsList.fullIterator();
				}

				{
					Query<MultiDatastream> mdsQuery = service.multiDatastreams().query()
							.filter(editorMultiDatastreamFilter.getValue())
							.top(1000)
							.expand("ObservedProperties($select=id,name,properties),Thing($select=id,name,properties),Observations($orderby=phenomenonTime desc;$top=1;$select=result,phenomenonTime)");
					EntityList<MultiDatastream> mdsList = mdsQuery.list();
					LOGGER.info("MultiDatastreams: {}", mdsList.size());
					multiDatastreams = mdsList.fullIterator();
				}

				String dsExcludeFilter = editorDatastreamExclude.getValue();
				if (!dsExcludeFilter.isEmpty()) {
					Query<Datastream> dsQuery = service.datastreams().query()
							.filter(dsExcludeFilter)
							.top(1000);
					EntityList<Datastream> dsList = dsQuery.list();
					LOGGER.info("Excluding {} Datastreams", dsList.size());
					for (Iterator<Datastream> it = dsList.fullIterator(); it.hasNext();) {
						Datastream ds = it.next();
						dsExclude.add(ds.getId());
					}
				}

				String mdsExcludeFilter = editorMultiDatastreamExclude.getValue();
				if (!mdsExcludeFilter.isEmpty()) {
					Query<MultiDatastream> dsQuery = service.multiDatastreams().query()
							.filter(mdsExcludeFilter)
							.top(1000);
					EntityList<MultiDatastream> dsList = dsQuery.list();
					LOGGER.info("Excluding {} MultiDatastreams", dsList.size());
					for (Iterator<MultiDatastream> it = dsList.fullIterator(); it.hasNext();) {
						MultiDatastream mds = it.next();
						mdsExclude.add(mds.getId());
					}
				}
			} catch (ServiceFailureException exc) {
				LOGGER.error("Failed.", exc);
				throw new ImportException(exc);
			}
		}

		private String fetchDocumentFor(Object awaaId, ZonedDateTime timeStart, ObservedProperty obsProp) throws IOException {
			ZonedDateTime timeEnd = timeStart.plusHours(editorMaxHours.getValue());

			Map<String, String> replaces = new HashMap<>();
			replaces.put("awaaId", translator.translate(awaaId.toString()));
			replaces.put("startTime", timeStart.format(timeFormatter));
			replaces.put("endTime", timeEnd.format(timeFormatter));
			replaces.put("obsPropId", translator.translate(String.valueOf(obsProp.getProperties().get("awaaId"))));
			replaces.put("obsPropName", translator.translate(obsProp.getName()));

			String targetUrl = editorFetchUrl.getValue();
			for (Map.Entry<String, String> entry : replaces.entrySet()) {
				String search = Pattern.quote("{" + entry.getKey() + "}");
				String replace = URLEncoder.encode(entry.getValue(), "UTF-8");
				targetUrl = targetUrl.replaceAll(search, replace);
			}

			LOGGER.debug("Fetching: {}", targetUrl);
			CloseableHttpClient client = HttpClients.createSystem();
			HttpGet get = new HttpGet(targetUrl);
			CloseableHttpResponse response = client.execute(get);
			String data = EntityUtils.toString(response.getEntity(), Charset.forName("UTF-8"));
			return data;
		}

		private List<Observation> computeForDatastreams() throws IOException, ServiceFailureException, ImportException {
			Datastream ds = datastreams.next();
			if (dsExclude.contains(ds.getId())) {
				LOGGER.info("Skipping Datastream {} {}", ds.getId(), ds.getName());
				return Collections.EMPTY_LIST;
			}
			Object awaaId = ds.getThing().getProperties().get("awaaId");

			ZonedDateTime timeStart = startTime.getInstant(ds).atZone(timeFormatter.getZone());

			ObservedProperty obsProp = ds.getObservedProperty();
			String data = fetchDocumentFor(awaaId, timeStart, obsProp);
			List<Observation> observations = docParser.process(ds, data);
			LOGGER.info("Generated {} observations for station {}, {}, {}", observations.size(), awaaId, timeStart, obsProp.getName());
			return observations;
		}

		private List<Observation> computeForMultiDatastreams() throws IOException, ServiceFailureException, ImportException {
			MultiDatastream mds = multiDatastreams.next();
			if (mdsExclude.contains(mds.getId())) {
				LOGGER.info("Skipping MultiDatastream {} {}", mds.getId(), mds.getName());
				return Collections.EMPTY_LIST;
			}
			Object awaaId = mds.getThing().getProperties().get("awaaId");

			ZonedDateTime timeStart = startTime.getInstant(mds).atZone(timeFormatter.getZone());

			List<ObservedProperty> observedProperties = mds.getObservedProperties().toList();
			String[] documents = new String[observedProperties.size()];
			for (int i = 0; i < documents.length; i++) {
				ObservedProperty obsProp = observedProperties.get(i);
				documents[i] = fetchDocumentFor(awaaId, timeStart, obsProp);
			}
			List<Observation> observations = docParser.process(mds, documents);
			LOGGER.info("Generated {} observations for station {}, {}, {}", observations.size(), awaaId, timeStart, mds.getName());
			return observations;
		}

		@Override
		public boolean hasNext() {
			return datastreams.hasNext() || multiDatastreams.hasNext();
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
			while (multiDatastreams.hasNext()) {
				try {
					return computeForMultiDatastreams();
				} catch (Exception exc) {
					LOGGER.error("Failed", exc);
				}
			}
			return Collections.emptyList();
		}
	}

	private static class Translator {

		private Map<String, String> replaces;

		public Translator(String json) throws IOException {
			ObjectMapper mapper = new ObjectMapper();
			replaces = mapper.readValue(json, new TypeReference<Map<String, String>>() {
			});
		}

		public String translate(String input) {
			String result = replaces.get(input);
			if (result == null) {
				return input;
			}
			return result;
		}

	}
}
