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
import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.ConfigurationException;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorClass;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class ImporterWrapper implements AnnotatedConfigurable<SensorThingsService, Object> {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ImporterWrapper.class);
	private final LoggingStatus logStatus = new LoggingStatus();
	private static final String NAME_DEFAULT = "Work";

	@ConfigurableField(editor = EditorSubclass.class, optional = false,
			label = "Importer", description = "The specific importer to use.")
	@EditorSubclass.EdOptsSubclass(iface = Importer.class)
	private Importer importer;

	@ConfigurableField(editor = EditorSubclass.class, optional = false,
			label = "Validator", description = "The validator to use.")
	@EditorSubclass.EdOptsSubclass(iface = Validator.class)
	private Validator validator;

	@ConfigurableField(editor = EditorClass.class, optional = false,
			label = "Uploader", description = "The class to use for uploading the observations to a server.")
	@EditorClass.EdOptsClass(clazz = ObservationUploader.class)
	private ObservationUploader uploader;

	@ConfigurableField(editor = EditorInt.class, optional = true,
			label = "Sleep Time", description = "Sleep for this number of ms after each insert.")
	@EditorInt.EdOptsInt(dflt = 0)
	private long sleep;

	@ConfigurableField(editor = EditorInt.class, optional = true,
			label = "ValidatorThreads", description = "The number of Threads to use for validation.")
	@EditorInt.EdOptsInt(dflt = 1)
	private int validatorThreads;

	@ConfigurableField(editor = EditorString.class, optional = false,
			label = "Name", description = "The name to use in log messages")
	@EditorString.EdOptsString(dflt = NAME_DEFAULT)
	private String name;

	private boolean noAct = false;
	private boolean doSleep;

	private long generated = 0;
	private AtomicLong validated = new AtomicLong();

	// Don't cache too many observations.
	private final long maxSend = 100000;
	private long nextSend = maxSend;

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx, ConfigEditor<?> configEditor) throws ConfigurationException {
		AnnotatedConfigurable.super.configure(config, context, edtCtx, configEditor);
		validator.setObservationUploader(uploader);
		logStatus.setName(name);

		if (validator == null) {
			validator = new Validator.ValidatorNull();
		}

		doSleep = sleep > 0;
	}

	public void setName(String name) {
		if (Utils.isNullOrEmpty(this.name) || !NAME_DEFAULT.equals(name)) {
			this.name = name;
			logStatus.setName("⏸" + name);
		}
	}

	private void doImport() {
		logStatus.setName("⏵" + name);
		Calendar start = Calendar.getInstance();
		try {
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
			uploader.sendDataArray();
		} catch (StatusCodeException exc) {
			LOGGER.error("URL: {}", exc.getUrl());
			LOGGER.error("Code: {} {}", exc.getStatusCode(), exc.getStatusMessage());
			LOGGER.error("Data: {}", exc.getReturnedContent());
			LOGGER.debug("Failed to import.", exc);
		} catch (ImportException | ServiceFailureException | RuntimeException exc) {
			LOGGER.error("Failed to import: {}", exc.getMessage());
			LOGGER.debug("Details:", exc);
		}
		logStatus.setInsertedCount(uploader.getInserted());
		logStatus.setUpdatedCount(uploader.getUpdated());
		logStatus.setDeletedCount(uploader.getDeleted());
		logStatus.setSpeed(getSpeed(start, validated.get()));
		logStatus.setName("⏹" + name);
	}

	private double getSpeed(Calendar since, long inserted) {
		Calendar now = Calendar.getInstance();
		double seconds = 1e-3 * (now.getTimeInMillis() - since.getTimeInMillis());
		return inserted / seconds;
	}

	private void validateAndSendObservations(Map<Object, List<Observation>> obsPerDs, Calendar start) throws ServiceFailureException, ImportException {
		ExecutorService executor = Executors.newFixedThreadPool(validatorThreads);
		AtomicLong queued = new AtomicLong();
		for (final List<Observation> observations : obsPerDs.values()) {
			logStatus.setQueuedCount(queued.incrementAndGet());
			executor.submit(() -> {
				validateAndSend(observations, start);
				logStatus.setQueuedCount(queued.decrementAndGet());
				return null;
			});
		}
		executor.shutdown();
		try {
			executor.awaitTermination(1, TimeUnit.DAYS);
		} catch (InterruptedException ex) {
			LOGGER.error("Interrupted waiting for tasks to finish.");
		}
		obsPerDs.clear();
	}

	private void validateAndSend(List<Observation> observations, Calendar start) throws ServiceFailureException, ImportException {
		for (Observation observation : observations) {
			if (validator.isValid(observation)) {
				uploader.addObservation(observation);
				logStatus.setValidatedCount(validated.incrementAndGet());
			}
			maybeSleep();
		}
		logStatus.setDeletedCount(uploader.getDeleted());
		logStatus.setInsertedCount(uploader.getInserted());
		logStatus.setUpdatedCount(uploader.getUpdated());
		logStatus.setSpeed(getSpeed(start, validated.get()));
	}

	private void maybeSleep() {
		if (doSleep) {
			try {
				Thread.sleep(sleep);
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
			configure(json, new SensorThingsService(), null, null);
			importer.setVerbose(noAct);
			importer.setNoAct(noAct);
			importer.setProgressTracker(tracker);
			uploader.setNoAct(noAct);
			doImport();
		} catch (JsonSyntaxException | ConfigurationException exc) {
			LOGGER.error("Failed to parse {}", config);
			LOGGER.debug("Failed to parse.", exc);
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

		public static final String MESSAGE = "{}: Read {}, Valid {}, New {}, Updated {}, Deleted {}, Queued {} - {}/s";
		public final Object[] status;

		public LoggingStatus() {
			super(MESSAGE, new Object[8]);
			status = getCurrentParams();
			Arrays.setAll(status, (int i) -> Long.valueOf(0));
			status[0] = "unnamed";
			status[7] = "0.0";
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

		public LoggingStatus setDeletedCount(Long count) {
			status[5] = count;
			return this;
		}

		public LoggingStatus setQueuedCount(Long count) {
			status[6] = count;
			return this;
		}

		public LoggingStatus setSpeed(Double speed) {
			status[7] = String.format("%.1f", speed);
			return this;
		}

	}
}
