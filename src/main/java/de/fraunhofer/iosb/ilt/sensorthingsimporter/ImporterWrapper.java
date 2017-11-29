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
import de.fraunhofer.iosb.ilt.configurable.Configurable;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorClass;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.validator.Validator;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Calendar;
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

	private SensorThingsService service;
	private boolean noAct = false;
	private Importer importer;
	private Validator validator;
	private ObservationUploader uploader;

	private int messageIntervalStart;
	private boolean doSleep;
	private long sleepTime;

	@Override
	public void configure(JsonElement config, Object context, Object edtCtx) {
		if (service == null) {
			service = new SensorThingsService();
		}
		getConfigEditor(service, edtCtx).setConfig(config);
		importer = editorImporter.getValue();
		uploader = editorUploader.getValue();
		validator = editorValidator.getValue();

		if (validator == null) {
			validator = new Validator.ValidatorNull();
		}

		sleepTime = editorSleepTime.getValue();
		doSleep = sleepTime > 0;
		messageIntervalStart = editorMsgInterval.getValue();
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
		}
		return editor;
	}

	private void doImport() throws ImportException, ServiceFailureException {
		int generated = 0;
		int validated = 0;
		int inserted;
		int nextMessage = messageIntervalStart;
		Calendar start = Calendar.getInstance();

		for (List<Observation> observations : importer) {
			for (Observation observation : observations) {
				generated++;
				if (validator.isValid(observation)) {
					validated++;
					uploader.addObservation(observation);
				}
				nextMessage--;
				if (nextMessage == 0) {
					inserted = uploader.sendDataArray();

					nextMessage = messageIntervalStart;
					Calendar now = Calendar.getInstance();
					double seconds = 1e-3 * (now.getTimeInMillis() - start.getTimeInMillis());
					double rowsPerSec = inserted / seconds;
					LOGGER.info("Genereated {}, Validated {}, Inserted {}, Updated {} Observations in {}s: {}/s.", generated, validated, inserted, uploader.getUpdated(), String.format("%.1f", seconds), String.format("%.1f", rowsPerSec));
				}
				maybeSleep();
			}
		}
		inserted = uploader.sendDataArray();

		Calendar now = Calendar.getInstance();
		double seconds = 1e-3 * (now.getTimeInMillis() - start.getTimeInMillis());
		double rowsPerSec = inserted / seconds;
		LOGGER.info("Genereated {}, Validated {}, Inserted {}, Updated {} observations in {}s ({}/s).", generated, validated, inserted, uploader.getUpdated(), String.format("%.1f", seconds), String.format("%.1f", rowsPerSec));
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
			doImport(config, noAct);
		} catch (IOException ex) {
			LOGGER.error("Failed to read config file.", ex);
		}
	}

	public void doImport(String config, boolean noAct) {
		this.noAct = noAct;
		try {
			JsonElement json = new JsonParser().parse(config);
			configure(json, null, null);
			importer.setVerbose(noAct);
			importer.setNoAct(noAct);
			uploader.setNoAct(noAct);
			doImport();
		} catch (JsonSyntaxException exc) {
			LOGGER.error("Failed to parse {}", config);
			LOGGER.debug("Failed to parse.", exc);
		} catch (ImportException | ServiceFailureException exc) {
			LOGGER.error("Failed to import.", exc);
		}
	}

	public static void importConfig(String config, boolean noAct) {
		ImporterWrapper wrapper = new ImporterWrapper();
		wrapper.doImport(config, noAct);
	}

	public static void importCmdLine(List<String> arguments) throws URISyntaxException, IOException, MalformedURLException, ServiceFailureException {
		Options options = new Options().parseArguments(arguments);

		ImporterWrapper wrapper = new ImporterWrapper();
		wrapper.doImport(options);
	}

}
