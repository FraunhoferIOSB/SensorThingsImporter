/*
 * Copyright (C) 2016 Fraunhofer Institut IOSB, Fraunhoferstr. 1, D 76131
 * Karlsruhe, Germany.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package de.fraunhofer.iosb.ilt.sensorthingsimporter;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import de.fraunhofer.iosb.ilt.sensorthingsimporter.Options.Option;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Options.Parameter;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.csv.ImporterCsv;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.csv.OptionsCsv;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class MainApp extends Application {

    @Override
    public void start(Stage stage) throws Exception {
        Parent root = FXMLLoader.load(getClass().getResource("/fxml/Scene.fxml"));

        Scene scene = new Scene(root);
        scene.getStylesheets().add("/styles/Styles.css");

        stage.setTitle("SensorThings CSV Importer");
        stage.setScene(scene);
        stage.show();
    }

    /**
     * The main() method is ignored in correctly deployed JavaFX application.
     * main() serves only as fallback in case the application can not be
     * launched through deployment artifacts, e.g., in IDEs with limited FX
     * support. NetBeans ignores main().
     *
     * @param args the command line arguments
     * @throws IOException
     * @throws URISyntaxException
     * @throws ServiceFailureException
     */
    public static void main(String[] args) throws IOException, URISyntaxException, ServiceFailureException {
        List<String> arguments = new ArrayList<>(Arrays.asList(args));
        if (arguments.isEmpty() || arguments.contains("-help")) {
            showHelp();
            System.exit(0);
        }

        if (arguments.contains("-gui")) {
            arguments.remove("-gui");
            Application.launch(args);
        } else {
            importCsv(arguments);
        }
        System.exit(0);
    }

    private static void showHelp() {
        OptionsCsv options = new OptionsCsv();
        for (Option option : options.getOptions()) {
            StringBuilder text = new StringBuilder();
            for (String key : option.getKeys()) {
                text.append(key).append(" ");
            }
            for (Parameter param : option.getParameters()) {
                text.append("[").append(param.getName()).append("] ");
            }
            text.append(":\n");
            for (String descLine : option.getDescription()) {
                text.append("    ").append(descLine).append("\n");
            }
            System.out.println(text.toString());
        }
    }

    private static void importCsv(List<String> arguments) throws URISyntaxException, IOException, MalformedURLException, ServiceFailureException {
        ImporterCsv importer = new ImporterCsv();
        importer.parseArguments(arguments);
        importer.doImport();
    }

}
