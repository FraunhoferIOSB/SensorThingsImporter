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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.csv;

import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.Configurable;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.auth.AuthMethod;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Entity;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ext.DataArrayDocument;
import de.fraunhofer.iosb.ilt.sta.model.ext.DataArrayValue;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class ImporterCsv implements Configurable<SensorThingsService, Object> {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ImporterCsv.class);
	private final OptionsCsv options = new OptionsCsv();
	private SensorThingsService service;
	private int messageIntervalStart;
	private boolean limitRows;
	private long rowLimit;
	private long rowSkip;
	private boolean doSleep;
	private long sleepTime;
	private boolean dataArray = false;

	private final RecordConverterCSV rcCsv = new RecordConverterCSV();

	private final Map<Entity, DataArrayValue> davMap = new HashMap<>();

	private Entity lastDatastream;
	private DataArrayValue lastDav;

	private EditorMap<SensorThingsService, Object, Map<String, Object>> editor;
	private EditorString editorService;
	private EditorBoolean editorUseDataArray;
	private EditorSubclass<Object, Object, AuthMethod> editorAuthMethod;

	private EditorInt editorRowLimit;
	private EditorInt editorRowSkip;
	private EditorInt editorSleepTime;
	private EditorInt editorMsgInterval;

	private EditorString editorCharSet;
	private EditorString editorInput;
	private EditorString editorDelimiter;
	private EditorBoolean editorTabDelim;

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx) {
		service = new SensorThingsService();
		getConfigEditor(service, edtCtx).setConfig(config, service, edtCtx);
		try {
			service.setEndpoint(new URI(editorService.getValue()));
		} catch (URISyntaxException ex) {
			LOGGER.error("Failed to create service.", ex);
			throw new IllegalArgumentException("Failed to create service.", ex);
		}
	}

	@Override
	public EditorMap<SensorThingsService, Object, Map<String, Object>> getConfigEditor(SensorThingsService context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorMap<>();

			editor.addOption("recordConvertor", rcCsv.getConfigEditor(context, edtCtx), false);

			editorService = new EditorString("https://service.somewhere/path/v1.0", 1, "Service URL", "The url of the server.");
			editor.addOption("serviceUrl", editorService, false);

			editorUseDataArray = new EditorBoolean(false, "Use DataArrays",
					"Use the SensorThingsAPI DataArray extension to post Observations. "
					+ "This is much more efficient when posting many observations. "
					+ "The number of items grouped together is determined by the messageInterval setting.");
			editor.addOption("useDataArrays", editorUseDataArray, true);

			editorAuthMethod = new EditorSubclass<>(AuthMethod.class, "Auth Method", "The authentication method the service uses.", false, "className");
			editor.addOption("authMethod", editorAuthMethod, true);

			editorRowLimit = new EditorInt(0, Integer.MAX_VALUE, 1, 0, "Row Limit", "The maximum number of rows to insert as observations (0=no limit).");
			editor.addOption("rowLimit", editorRowLimit, true);

			editorRowSkip = new EditorInt(0, Integer.MAX_VALUE, 1, 0, "Row Skip", "The number of rows to skip when reading the file (0=none).");
			editor.addOption("rowSkip", editorRowSkip, true);

			editorSleepTime = new EditorInt(0, Integer.MAX_VALUE, 1, 0, "Sleep Time", "Sleep for this number of ms after each insert.");
			editor.addOption("sleep", editorSleepTime, true);

			editorMsgInterval = new EditorInt(0, Integer.MAX_VALUE, 1, 10000, "Message Interval", "Output a progress message every [interval] records. Defaults to 10000");
			editor.addOption("msgInterval", editorMsgInterval, true);

			editorCharSet = new EditorString("UTF-8", 1, "Characterset", "The character set to use when parsing the csv file (default UTF-8).");
			editor.addOption("charset", editorCharSet, true);

			editorInput = new EditorString("", 1, "InputUrl", "The path or URL to the csv file.");
			editor.addOption("input", editorInput, false);

			editorDelimiter = new EditorString("", 1, "delimiter", "The delimiter to use instead of comma.");
			editor.addOption("delimiter", editorDelimiter, true);

			editorTabDelim = new EditorBoolean(false, "Tab Delimited", "Use tab as delimiter instead of comma.");
			editor.addOption("tab", editorTabDelim, true);
		}
		return editor;
	}

	public void parseArguments(List<String> args) {
		options.parseArguments(args);
	}

	public void doImport() throws MalformedURLException, URISyntaxException, ServiceFailureException, IOException {
		boolean noAct = options.getNoAct().isSet();
		if (noAct) {
			LOGGER.info("Not making any changes.");
		}
		rcCsv.setVerbose(noAct);

		dataArray = editorUseDataArray.getValue();

		editorAuthMethod.getValue().setAuth(service);

		limitRows = false;
		rowLimit = editorRowLimit.getValue();
		if (rowLimit > 0) {
			limitRows = true;
		}
		rowSkip = editorRowSkip.getValue();

		doSleep = false;
		sleepTime = editorSleepTime.getValue();
		if (sleepTime > 0) {
			doSleep = true;
		}

		CSVFormat format = CSVFormat.DEFAULT;
		if (editorTabDelim.getValue()) {
			format = format.withDelimiter('\t');
		}
		String delim = editorDelimiter.getValue();
		if (delim.length() > 0) {
			format = format.withDelimiter(delim.charAt(0));
		}

		String input = editorInput.getValue();
		URL inUrl = null;
		try {
			inUrl = new URL(input);
		} catch (MalformedURLException e) {
		}
		File inFile = null;
		try {
			inFile = new File(input);
		} catch (Exception e) {
		}

		String charset = editorCharSet.getValue();
		CSVParser parser;
		if (inUrl != null) {
			parser = CSVParser.parse(inUrl, Charset.forName(charset), format);
		} else if (inFile != null) {
			parser = CSVParser.parse(inFile, Charset.forName(charset), format);
		} else {
			throw new IllegalArgumentException("No valid input url or file.");
		}

		messageIntervalStart = editorMsgInterval.getValue();

		importLoop(parser, noAct);
	}

	private void importLoop(CSVParser parser, boolean noAct) {
		LOGGER.info("Reading {} rows (0=∞), skipping {} rows.", rowLimit, rowSkip);
		int rowCount = 0;
		int totalCount = 0;
		int inserted = 0;
		Calendar start = Calendar.getInstance();
		int nextMessage = messageIntervalStart;

		try {
			for (CSVRecord record : parser) {
				totalCount++;
				if (rowSkip > 0) {
					rowSkip--;
					continue;
				}
				Observation obs = rcCsv.convert(record);
				if (!dataArray && !noAct) {
					service.create(obs);
					inserted++;
				}
				if (dataArray) {
					addToDataArray(obs);
				}

				rowCount++;
				if (limitRows && rowCount > rowLimit) {
					break;
				}
				nextMessage--;
				if (nextMessage == 0) {
					if (dataArray) {
						inserted += sendDataArray(noAct);
					}

					nextMessage = messageIntervalStart;
					Calendar now = Calendar.getInstance();
					double seconds = 1e-3 * (now.getTimeInMillis() - start.getTimeInMillis());
					double rowsPerSec = inserted / seconds;
					LOGGER.info("Processed {} rows in {}s: {}/s.", inserted, String.format("%.1f", seconds), String.format("%.1f", rowsPerSec));
				}
				maybeSleep();
			}
			if (dataArray) {
				inserted += sendDataArray(noAct);
			}
		} catch (Exception e) {
			LOGGER.error("Exception:", e);
		}
		Calendar now = Calendar.getInstance();
		double seconds = 1e-3 * (now.getTimeInMillis() - start.getTimeInMillis());
		double rowsPerSec = inserted / seconds;
		LOGGER.info("Parsed {} rows of {}, inserted {} observations in {}s ({}/s).", rowCount, totalCount, inserted, String.format("%.1f", seconds), String.format("%.1f", rowsPerSec));
	}

	private void addToDataArray(Observation o) throws ServiceFailureException {
		Entity ds = o.getDatastream();
		if (ds == null) {
			ds = o.getMultiDatastream();
		}
		if (ds == null) {
			throw new IllegalArgumentException("Observation must have a (Multi)Datastream.");
		}
		if (ds != lastDatastream) {
			findDataArrayValue(ds);
		}
		lastDav.addObservation(o);
	}

	private void findDataArrayValue(Entity ds) {
		DataArrayValue dav = davMap.get(ds);
		if (dav == null) {
			if (ds instanceof Datastream) {
				dav = new DataArrayValue((Datastream) ds, rcCsv.getDefinedProperties());
			} else {
				dav = new DataArrayValue((MultiDatastream) ds, rcCsv.getDefinedProperties());
			}
			davMap.put(ds, dav);
		}
		lastDav = dav;
		lastDatastream = ds;
	}

	private int sendDataArray(boolean noAct) throws ServiceFailureException {
		int inserted = 0;
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
			inserted += nonError;
		}
		davMap.clear();
		lastDav = null;
		lastDatastream = null;
		return inserted;
	}

	private void maybeSleep() {
		if (doSleep) {
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException ex) {
				LOGGER.info("Rude wakeup.", ex);
			}
		}
	}

}
