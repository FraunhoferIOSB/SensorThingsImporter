/*
 * Copyright (C) 2017 Fraunhofer Institut IOSB, Fraunhoferstr. 1, D 76131
 * Karlsruhe, Germany.
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
package de.fraunhofer.iosb.ilt.sensorthingsimporter;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.Configurable;
import de.fraunhofer.iosb.ilt.configurable.ConfigurationException;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorClass;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.scheduler.ImporterScheduler;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ChangingStatusLogger;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ProgressTracker;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.validator.Validator;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.StatusCodeException;
import de.fraunhofer.iosb.ilt.sta.Utils;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class ImporterWrapper implements Configurable<Object, Object> {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ImporterWrapper.class);

	private EditorMap<Map<String, Object>> editor;
	private EditorSubclass<Object, Object, Importer> editorImporter;
	private EditorSubclass<SensorThingsService, Object, Validator> editorValidator;
	private EditorClass<SensorThingsService, Object, ObservationUploader> editorUploader;
	private EditorInt editorSleepTime;
	private EditorInt editorMsgInterval;
	private EditorString editorName;

	private SensorThingsService service;
	private boolean noAct = false;
	private Importer importer;
	private Validator validator;
	private ObservationUploader uploader;

	private int messageIntervalStart;
	private boolean doSleep;
	private long sleepTime;
	private final LoggingStatus logStatus = new LoggingStatus();

	private long generated = 0;
	private long validated = 0;
	private long inserted;
	private long nextMessage;

	// Don't cache too many observations.
	private final long maxSend = 100000;
	private long nextSend = maxSend;

	private String name;

	@Override
	public void configure(JsonElement config, Object context, Object edtCtx, ConfigEditor<?> configEditor) {
		try {
			service = new SensorThingsService();
			getConfigEditor(service, edtCtx).setConfig(config);
			importer = editorImporter.getValue();
			uploader = editorUploader.getValue();
			validator = editorValidator.getValue();
			if (!editorName.isDefault()) {
				name = editorName.getValue();
				logStatus.setName(name);
			}

			if (validator == null) {
				validator = new Validator.ValidatorNull();
			}

			sleepTime = editorSleepTime.getValue();
			doSleep = sleepTime > 0;
			messageIntervalStart = editorMsgInterval.getValue();
		} catch (ConfigurationException ex) {
			throw new IllegalStateException(ex);
		}
	}

	@Override
	public EditorMap<Map<String, Object>> getConfigEditor(Object context, Object edtCtx) {
		if (editor == null) {
			if (service == null) {
				service = new SensorThingsService();
			}
			editor = new EditorMap<>();

			editorImporter = new EditorSubclass(service, edtCtx, Importer.class, "Importer", "The specific importer to use.", false, "className");
			editor.addOption("importer", editorImporter, false);

			editorValidator = new EditorSubclass(service, edtCtx, Validator.class, "Validator", "The validator to use.", false, "className");
			editor.addOption("validator", editorValidator, true);

			editorUploader = new EditorClass(service, edtCtx, ObservationUploader.class, "Uploader", "The class to use for uploading the observations to a server.");
			editor.addOption("uploader", editorUploader, false);

			editorSleepTime = new EditorInt(0, Integer.MAX_VALUE, 1, 0, "Sleep Time", "Sleep for this number of ms after each insert.");
			editor.addOption("sleep", editorSleepTime, true);

			editorMsgInterval = new EditorInt(0, Integer.MAX_VALUE, 1, 10000, "Message Interval", "Output a progress message every [interval] records. Defaults to 10000");
			editor.addOption("msgInterval", editorMsgInterval, true);

			editorName = new EditorString("", 1, "Name", "The name to use in log messages");
			editor.addOption("name", editorName, true);
		}
		return editor;
	}

	public void setName(String name) {
		if (Utils.isNullOrEmpty(this.name)) {
			this.name = name;
			logStatus.setName("⏸" + name);
		}
	}

	private void doImport() throws ImportException, ServiceFailureException {
		logStatus.setName("⏵" + name);
		nextMessage = messageIntervalStart;
		Calendar start = Calendar.getInstance();

		// Map of Obs per Ds/MDs
		Map<Object, List<Observation>> obsPerDs = new HashMap<>();

		for (List<Observation> observations : importer) {
			for (Observation observation : observations) {
				Object key = observation.getDatastream();
				if (key == null) {
					key = observation.getMultiDatastream();
				}
				List<Observation> obsList = obsPerDs.computeIfAbsent(key, t -> new ArrayList<>());
				obsList.add(observation);
				logStatus.setGeneratedCount(++generated);
				nextSend--;
			}
			if (nextSend <= 0) {
				validateAndSendObservations(obsPerDs, start);
				nextSend = maxSend;
			}
		}

		validateAndSendObservations(obsPerDs, start);
		inserted = uploader.sendDataArray();
		logStatus.setInsertedCount(inserted);
		logStatus.setUpdatedCount(Long.valueOf(uploader.getUpdated()));
		logStatus.setSpeed(getSpeed(start, inserted));
		logStatus.setName("⏹" + name);
	}

	private double getSpeed(Calendar since, long inserted) {
		Calendar now = Calendar.getInstance();
		double seconds = 1e-3 * (now.getTimeInMillis() - since.getTimeInMillis());
		return inserted / seconds;
	}

	private void validateAndSendObservations(Map<Object, List<Observation>> obsPerDs, Calendar start) throws ServiceFailureException, ImportException {
		for (List<Observation> observations : obsPerDs.values()) {
			for (Observation observation : observations) {
				if (validator.isValid(observation)) {
					validated++;
					uploader.addObservation(observation);
					logStatus.setValidatedCount(validated);
				}
				nextMessage--;
				if (nextMessage <= 0) {
					inserted = uploader.sendDataArray();
					nextMessage = messageIntervalStart;
					logStatus.setInsertedCount(inserted);
					logStatus.setUpdatedCount(Long.valueOf(uploader.getUpdated()));
					logStatus.setSpeed(getSpeed(start, inserted));
				}
				maybeSleep();
			}
		}
		obsPerDs.clear();
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

	public void doImport(Options options) {
		this.noAct = options.getNoAct().isSet();
		String fileName = options.getFileName().getValue();
		File configFile = new File(fileName);
		try {
			String config = FileUtils.readFileToString(configFile, "UTF-8");
			doImport(config, noAct, null);
		} catch (IOException ex) {
			LOGGER.error("Failed to read config file.", ex);
		}
	}

	public void doImport(String config, boolean noAct, ProgressTracker tracker) {
		this.noAct = noAct;
		if (tracker == null) {
			tracker = (p, t) -> {
			};
		}
		ImporterScheduler.STATUS_LOGGER.addLogStatus(logStatus);
		try {
			JsonElement json = JsonParser.parseString(config);
			configure(json, null, null, null);
			importer.setVerbose(noAct);
			importer.setNoAct(noAct);
			importer.setProgressTracker(tracker);
			uploader.setNoAct(noAct);
			doImport();
		} catch (JsonSyntaxException exc) {
			LOGGER.error("Failed to parse {}", config);
			LOGGER.debug("Failed to parse.", exc);
		} catch (StatusCodeException exc) {
			LOGGER.error("URL: " + exc.getUrl());
			LOGGER.error("Code: " + exc.getStatusCode() + " " + exc.getStatusMessage());
			LOGGER.error("Data: " + exc.getReturnedContent());
			LOGGER.error("Failed to import.", exc);
		} catch (ImportException | ServiceFailureException | RuntimeException exc) {
			LOGGER.error("Failed to import.", exc);
		}
		ImporterScheduler.STATUS_LOGGER.removeLogStatus(logStatus);
	}

	public static void importConfig(String config, boolean noAct, ProgressTracker tracker) {
		ImporterWrapper wrapper = new ImporterWrapper();
		wrapper.doImport(config, noAct, tracker);
	}

	public static void importCmdLine(List<String> arguments) throws URISyntaxException, IOException, MalformedURLException, ServiceFailureException {
		Options options = new Options().parseArguments(arguments);

		ImporterWrapper wrapper = new ImporterWrapper();
		wrapper.doImport(options);
	}

	private static class LoggingStatus extends ChangingStatusLogger.ChangingStatusDefault {

		public static final String MESSAGE = "{}: Genereated {}, Validated {}, Inserted {}, Updated {}, {}/s";
		public final Object[] status;

		public LoggingStatus() {
			super(MESSAGE, new Object[6]);
			status = getCurrentParams();
			Arrays.setAll(status, (int i) -> Long.valueOf(0));
			status[0] = "unnamed";
			status[5] = "0.0";
		}

		public LoggingStatus setName(String name) {
			status[0] = name;
			return this;
		}

		public LoggingStatus setGeneratedCount(Long count) {
			status[1] = count;
			return this;
		}

		public LoggingStatus setValidatedCount(Long count) {
			status[2] = count;
			return this;
		}

		public LoggingStatus setInsertedCount(Long count) {
			status[3] = count;
			return this;
		}

		public LoggingStatus setUpdatedCount(Long count) {
			status[4] = count;
			return this;
		}

		public LoggingStatus setSpeed(Double speed) {
			status[5] = String.format("%.1f", speed);
			return this;
		}

	}
}
