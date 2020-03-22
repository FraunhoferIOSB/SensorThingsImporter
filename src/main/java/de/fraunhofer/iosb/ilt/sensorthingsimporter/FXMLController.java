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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import de.fraunhofer.iosb.ilt.configurable.ConfigurationException;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.scheduler.ImporterScheduler;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ProgressTracker;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javafx.concurrent.Task;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.ScrollPane;
import javafx.stage.FileChooser;
import org.apache.commons.io.FileUtils;
import org.quartz.SchedulerException;
import org.slf4j.LoggerFactory;

public class FXMLController implements Initializable {

	/**
	 * The logger for this class.
	 */
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FXMLController.class);
	@FXML
	private ScrollPane paneConfig;
	@FXML
	private Button buttonLoad;
	@FXML
	private Button buttonSave;
	@FXML
	private Button buttonImport;
	@FXML
	private CheckBox toggleNoAct;
	@FXML
	private CheckBox toggleScheduler;
	@FXML
	private ProgressBar progressBar;

	private ImporterWrapper wrapper;
	private ImporterScheduler scheduler;

	private EditorMap<?> configEditorImport;
	private EditorMap<?> configEditorSchedule;
	private FileChooser fileChooser = new FileChooser();

	private ExecutorService executor = Executors.newFixedThreadPool(1);

	@FXML
	private void actionLoad(ActionEvent event) throws ConfigurationException {
		if (toggleScheduler.isSelected()) {
			loadScheduler();
		} else {
			loadImporter();
		}
	}

	private void loadImporter() {
		JsonElement json = loadFromFile("Load Importer");
		if (json == null) {
			return;
		}
		wrapper = new ImporterWrapper();
		configEditorImport = wrapper.getConfigEditor(null, null);
		replaceEditor();
		wrapper.configure(json, null, null, null);
	}

	private void loadScheduler() throws ConfigurationException {
		JsonElement json = loadFromFile("Load Scheduler");
		if (json == null) {
			return;
		}
		scheduler = new ImporterScheduler();
		configEditorSchedule = scheduler.getConfigEditor(null, null);
		replaceEditor();
		scheduler.configure(json, null, null, null);
	}

	private JsonElement loadFromFile(String title) {
		try {
			fileChooser.setTitle(title);
			File file = fileChooser.showOpenDialog(paneConfig.getScene().getWindow());
			if (file == null) {
				return null;
			}
			String config = FileUtils.readFileToString(file, "UTF-8");
			JsonElement json = new JsonParser().parse(config);
			return json;
		} catch (IOException ex) {
			LOGGER.error("Failed to read file", ex);
			Alert alert = new Alert(Alert.AlertType.ERROR);
			alert.setTitle("failed to read file");
			alert.setContentText(ex.getLocalizedMessage());
			alert.showAndWait();
		}
		return null;
	}

	@FXML
	private void actionSave(ActionEvent event) {
		if (toggleScheduler.isSelected()) {
			saveScheduler();
		} else {
			saveImporter();
		}
	}

	private void saveImporter() {
		JsonElement json = configEditorImport.getConfig();
		saveToFile(json, "Save Importer");
	}

	private void saveScheduler() {
		JsonElement json = configEditorSchedule.getConfig();
		saveToFile(json, "Save Schedule");
	}

	private void saveToFile(JsonElement json, String title) {
		String config = new GsonBuilder().setPrettyPrinting().create().toJson(json);
		fileChooser.setTitle(title);
		File file = fileChooser.showSaveDialog(paneConfig.getScene().getWindow());

		try {
			FileUtils.writeStringToFile(file, config, "UTF-8");
		} catch (IOException ex) {
			LOGGER.error("Failed to write file.", ex);
			Alert alert = new Alert(Alert.AlertType.ERROR);
			alert.setTitle("failed to write file");
			alert.setContentText(ex.getLocalizedMessage());
			alert.showAndWait();
		}
	}

	@FXML
	private void actionImport(ActionEvent event) throws ConfigurationException {
		buttonImport.setDisable(true);

		Task<Void> task = new Task<Void>() {
			@Override
			protected Void call() throws Exception {
				updateProgress(0, 100);
				try {
					runImport(this::updateProgress);
				} catch (ConfigurationException | RuntimeException ex) {
					LOGGER.error("Failed to import.", ex);
				}
				updateProgress(100, 100);
				importDone();
				return null;
			}
		};
		progressBar.progressProperty().bind(task.progressProperty());
		executor.submit(task);
	}

	private void runImport(ProgressTracker tracker) throws ConfigurationException {
		if (toggleScheduler.isSelected()) {
			scheduler.setNoAct(toggleNoAct.isSelected());
			JsonElement json = configEditorSchedule.getConfig();
			String config = new Gson().toJson(json);
			scheduler.setConfig(config);
			try {
				scheduler.start();
			} catch (SchedulerException ex) {
				LOGGER.error("Exception starting scheduler", ex);
			}
		} else {
			JsonElement json = configEditorImport.getConfig();
			String config = new Gson().toJson(json);
			ImporterWrapper.importConfig(config, toggleNoAct.isSelected(), tracker);
		}
	}

	private void importDone() {
		buttonImport.setDisable(false);
	}

	@FXML
	private void actionScheduler(ActionEvent event) {
		replaceEditor();
	}

	private void replaceEditor() {
		if (toggleScheduler.isSelected()) {
			paneConfig.setContent(configEditorSchedule.getGuiFactoryFx().getNode());
		} else {
			paneConfig.setContent(configEditorImport.getGuiFactoryFx().getNode());
		}
	}

	@Override
	public void initialize(URL url, ResourceBundle rb) {
		wrapper = new ImporterWrapper();
		configEditorImport = wrapper.getConfigEditor(null, null);

		scheduler = new ImporterScheduler();
		configEditorSchedule = scheduler.getConfigEditor(null, null);

		replaceEditor();
	}

	public void close() {
		executor.shutdown();
	}
}
