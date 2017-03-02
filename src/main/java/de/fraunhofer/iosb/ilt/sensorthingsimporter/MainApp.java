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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fraunhofer.iosb.ilt.sensorthingsimporter.Options.Option;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Options.OptionDouble;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Options.Parameter;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import javafx.application.Application;
import static javafx.application.Application.launch;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class MainApp extends Application {

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MainApp.class);

    @Override
    public void start(Stage stage) throws Exception {
        Parent root = FXMLLoader.load(getClass().getResource("/fxml/Scene.fxml"));

        Scene scene = new Scene(root);
        scene.getStylesheets().add("/styles/Styles.css");

        stage.setTitle("JavaFX and Maven");
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
     */
    public static void main(String[] args) throws IOException, URISyntaxException, ServiceFailureException {
        List<String> arguments = new ArrayList<>(Arrays.asList(args));
        if (arguments.contains("-help")) {
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
            System.exit(0);
        }

        if (arguments.contains("-gui")) {
            arguments.remove("-gui");
            launch(args);
        } else {
            OptionsCsv options = new OptionsCsv().parseArguments(arguments);
            boolean noAct = options.getNoAct().isSet();

            String serverUrl = options.getServerUrl().getValue();
            LOGGER.info("Using service: {}", serverUrl);
            URL url = new URL(serverUrl);
            SensorThingsService service = new SensorThingsService(url);

            if (options.getBasicAuth().isSet()) {
                setBasicAuth(options, url, service);
            }

            DatastreamMapper datastreamMapper = options.createDatastreamMapper(service);

            RecordConverterCSV rcv = new RecordConverterCSV(options, datastreamMapper);
            rcv.setVerbose(noAct);

            boolean limitRows = false;
            long rowLimit = options.getRowLimit().getValue();
            if (rowLimit > 0) {
                limitRows = true;
            }
            long rowSkip = options.getRowSkip().getValue();

            boolean doSleep = false;
            long sleepTime = options.getSleep().getValue();
            if (sleepTime > 0) {
                doSleep = true;
            }

            File inFile = new File(options.getFileName().getValue());
            CSVFormat format = CSVFormat.DEFAULT;
            if (options.getTabDelimited().isSet()) {
                format = format.withDelimiter('\t');
            }
            CSVParser parser = CSVParser.parse(inFile, Charset.forName(options.getCharset().getValue()), format);

            int messageIntervalStart = options.getMessageInterval().getValue();
            int nextMessage = messageIntervalStart;

            LOGGER.info("Reading {} rows (0=∞), skipping {} rows.", rowLimit, rowSkip);
            int rowCount = 0;
            int totalCount = 0;
            int inserted = 0;
            Calendar last = Calendar.getInstance();

            try {
                for (CSVRecord record : parser) {
                    totalCount++;
                    if (rowSkip > 0) {
                        rowSkip--;
                        continue;
                    }
                    Observation obs = rcv.convert(record);
                    if (!noAct) {
                        service.create(obs);
                        inserted++;
                    }

                    rowCount++;
                    if (limitRows && rowCount > rowLimit) {
                        break;
                    }
                    nextMessage--;
                    if (nextMessage == 0) {
                        nextMessage = messageIntervalStart;
                        Calendar now = Calendar.getInstance();
                        double seconds = 1e-3 * (now.getTimeInMillis() - last.getTimeInMillis());
                        LOGGER.info("Processed {} rows in {}s.", inserted, seconds);
                        last = now;
                    }
                    if (doSleep) {
                        try {
                            Thread.sleep(sleepTime);
                        } catch (InterruptedException ex) {
                            LOGGER.info("Rude wakeup.", ex);
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Exception:", e);
            }
            LOGGER.info("Parsed {} rows of {}, inserted {} observations.", rowCount, totalCount, inserted);
        }
        System.exit(0);
    }

    private static void setBasicAuth(OptionsCsv options, URL url, SensorThingsService service) {
        OptionDouble<String, String> basicAuth = options.getBasicAuth();
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
                new AuthScope(url.getHost(), url.getPort()),
                new UsernamePasswordCredentials(basicAuth.getValue1(), basicAuth.getValue2()));
        CloseableHttpClient httpclient = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .build();
        service.setClient(httpclient);
    }

}
