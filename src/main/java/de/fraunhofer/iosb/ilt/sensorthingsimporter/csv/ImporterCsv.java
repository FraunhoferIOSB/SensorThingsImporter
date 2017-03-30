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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
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
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fraunhofer.iosb.ilt.sensorthingsimporter.DatastreamMapper;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Options.OptionDouble;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Entity;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ext.DataArrayDocument;
import de.fraunhofer.iosb.ilt.sta.model.ext.DataArrayValue;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;

/**
 *
 * @author scf
 */
public class ImporterCsv {

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ImporterCsv.class);
    private final OptionsCsv options = new OptionsCsv();
    private URL url;
    private SensorThingsService service;
    private int messageIntervalStart;
    private boolean limitRows;
    private long rowLimit;
    private long rowSkip;
    private boolean doSleep;
    private long sleepTime;
    private boolean dataArray = false;
    private RecordConverterCSV rcCsv;
    private Map<Entity, DataArrayValue> davMap = new HashMap<>();
    private Entity lastDatastream;
    private DataArrayValue lastDav;

    public void parseArguments(List<String> args) {
        options.parseArguments(args);
    }

    public void doImport() throws MalformedURLException, URISyntaxException, ServiceFailureException, IOException {
        boolean noAct = options.getNoAct().isSet();
        if (noAct) {
            LOGGER.info("Not making any changes.");
        }

        dataArray = options.getUseDataArray().isSet();

        String serverUrl = options.getServerUrl().getValue();
        LOGGER.info("Using service: {}", serverUrl);
        url = new URL(serverUrl);
        service = new SensorThingsService(url);

        if (options.getBasicAuth().isSet()) {
            setBasicAuth();
        }

        DatastreamMapper datastreamMapper = options.createDatastreamMapper(service);

        rcCsv = new RecordConverterCSV(options, datastreamMapper);
        rcCsv.setVerbose(noAct);

        limitRows = false;
        rowLimit = options.getRowLimit().getValue();
        if (rowLimit > 0) {
            limitRows = true;
        }
        rowSkip = options.getRowSkip().getValue();

        doSleep = false;
        sleepTime = options.getSleep().getValue();
        if (sleepTime > 0) {
            doSleep = true;
        }

        File inFile = new File(options.getFileName().getValue());
        CSVFormat format = CSVFormat.DEFAULT;
        if (options.getTabDelimited().isSet()) {
            format = format.withDelimiter('\t');
        }
        CSVParser parser = CSVParser.parse(inFile, Charset.forName(options.getCharset().getValue()), format);

        messageIntervalStart = options.getMessageInterval().getValue();

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

    private void setBasicAuth() {
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
