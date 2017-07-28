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

import com.google.common.collect.AbstractIterator;
import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Importer;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.TimeObject;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

/**
 *
 * @author scf
 */
public class ImporterUniPg implements Importer {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ImporterUniPg.class);

	private EditorMap<Map<String, Object>> editor;
	private EditorSubclass<SensorThingsService, Object, DocumentParser> editorDocumentParser;
	private EditorString editorFetchUrl;
	private EditorInt editorDuration;

	private SensorThingsService service;
	private DocumentParser docParser;
	private boolean verbose;

	@Override
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx) {
		service = context;
		getConfigEditor(context, edtCtx).setConfig(config);
		docParser = editorDocumentParser.getValue();
	}

	@Override
	public ConfigEditor<?> getConfigEditor(SensorThingsService context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorMap<>();

			editorDocumentParser = new EditorSubclass<>(context, edtCtx, DocumentParser.class, "DocumentParser", "The parser that transforms a document into Observations.");
			editor.addOption("documentParser", editorDocumentParser, false);

			editorFetchUrl = new EditorString("File://Accel_3.lvm", 1, "fetchUrl", "The url to fetch data from.");
			editor.addOption("fetchUrl", editorFetchUrl, false);

			editorDuration = new EditorInt(0, 99999, 1, 30 * 60, "PhenomenonTimeDuration", "Seconds between start and end phenomenonTime.");
			editor.addOption("duration", editorDuration, true);
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

	private class ObsListIter extends AbstractIterator<List<Observation>> {

		boolean done = false;

		public ObsListIter() throws ImportException {
		}

		private String fetchDocument() throws IOException {
			String targetUrl = editorFetchUrl.getValue();
			LOGGER.debug("Fetching: {}", targetUrl);
			URL url = new URL(targetUrl);
			BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));
			String inputLine;
			StringBuilder data = new StringBuilder();
			while ((inputLine = in.readLine()) != null) {
				data.append(inputLine).append('\n');
			}
			in.close();
			return data.toString();
		}

		private List<Observation> compute() throws IOException, ImportException {
			String data = fetchDocument();
			List<Observation> observations = docParser.process(null, data);
			LOGGER.info("Generated {} observations.", observations.size());
			TimeObject phenTime = new TimeObject(Interval.of(Instant.parse("2016-01-01T15:00:00Z"), Instant.parse("2016-01-01T15:30:00Z")));
			for (Observation obs : observations) {
				obs.setPhenomenonTime(phenTime);
			}
			return observations;
		}

		@Override
		protected List<Observation> computeNext() {
			if (!done) {
				try {
					done = true;
					return compute();
				} catch (ImportException | IOException exc) {
					LOGGER.error("Failed", exc);
				}
			}
			return endOfData();
		}
	}

}
