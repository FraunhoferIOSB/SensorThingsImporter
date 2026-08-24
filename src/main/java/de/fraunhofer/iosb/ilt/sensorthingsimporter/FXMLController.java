/*
 * Copyright (C) 2026 Fraunhofer Institut IOSB, Fraunhoferstr. 1, D 76131
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
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditors;
import de.fraunhofer.iosb.ilt.configurable.ConfigurationException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.scheduler.ImporterScheduler;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ProgressTracker;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javafx.concurrent.Task;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.Node;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.ScrollPane;
import javafx.scene.input.Dragboard;
import javafx.scene.input.TransferMode;
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

    private ImporterScheduler schedulerActive;

    private ConfigEditor<?> configEditorImport;
    private ConfigEditor<?> configEditorSchedule;
    private final FileChooser fileChooser = new FileChooser();
    private File openedFile;

    private final ExecutorService executor = Executors.newFixedThreadPool(1);

    @FXML
    private void actionLoad(ActionEvent event) throws ConfigurationException {
        if (toggleScheduler.isSelected()) {
            loadFromFile("Load Scheduler");
        } else {
            loadFromFile("Load Importer");
        }
    }

    private void loadFromFile(String title) {
        fileChooser.setTitle(title);
        File file = fileChooser.showOpenDialog(paneConfig.getScene().getWindow());
        loadFromFile(file);
    }

    private void loadFromFile(File file) {
        if (file == null) {
            return;
        }
        openedFile = file;
        String config;
        try {
            config = FileUtils.readFileToString(file, "UTF-8");
        } catch (IOException ex) {
            LOGGER.error("Failed to read file", ex);
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle("failed to read file");
            alert.setContentText(ex.getLocalizedMessage());
            alert.showAndWait();
            return;
        }
        JsonElement json = JsonParser.parseString(config);
        if (json == null) {
            return;
        }
        if (toggleScheduler.isSelected()) {
            configEditorSchedule = ConfigEditors
                    .buildEditorFromClass(ImporterScheduler.class, null, null)
                    .get();
            configEditorSchedule.setConfig(json);
        } else {
            configEditorImport = ConfigEditors
                    .buildEditorFromClass(ImporterWrapper.class, null, null)
                    .get();
            configEditorImport.setConfig(json);
        }
        replaceEditor();
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
        fileChooser.setInitialDirectory(openedFile.getParentFile());
        fileChooser.setInitialFileName(openedFile.getName());
        File file = fileChooser.showSaveDialog(paneConfig.getScene().getWindow());
        if (file == null) {
            return;
        }
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
            schedulerActive = new ImporterScheduler();
            schedulerActive.setNoAct(toggleNoAct.isSelected());
            JsonElement json = configEditorSchedule.getConfig();
            String config = new Gson().toJson(json);
            schedulerActive.setConfig(config);
            try {
                schedulerActive.start();
            } catch (SchedulerException ex) {
                LOGGER.error("Exception starting scheduler", ex);
            }
        } else {
            ImporterScheduler.STATUS_LOGGER.start();
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
        configEditorImport = ConfigEditors
                .buildEditorFromClass(ImporterWrapper.class, null, null)
                .get();

        configEditorSchedule = ConfigEditors
                .buildEditorFromClass(ImporterScheduler.class, null, null)
                .get();
        makeDropTarget(paneConfig, this::loadFromFile);

        replaceEditor();
    }

    public void close() {
        LOGGER.info("Received close, shutting down executor.");
        ImporterScheduler.STATUS_LOGGER.stop();
        List<Runnable> remaining = executor.shutdownNow();
        LOGGER.info("Remaining threads: {}", remaining.size());
    }

    public static void makeDropTarget(Node node, FileAction action) {
        node.setOnDragOver(event -> {
            if (event.getGestureSource() != node && event.getDragboard().hasFiles()) {
                event.acceptTransferModes(TransferMode.COPY);
            }
            event.consume();
        });
        node.setOnDragDropped(event -> {
            Dragboard db = event.getDragboard();
            boolean success = false;
            if (db.hasFiles()) {
                List<File> files = db.getFiles();
                for (var file : files) {
                    action.call(file);
                }
                success = true;
            }
            event.setDropCompleted(success);
            event.consume();
        });
    }

    public static interface FileAction {

        abstract public void call(File file);
    }

}
