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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.csv;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fraunhofer.iosb.ilt.sensorthingsimporter.DatastreamMapper;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.DsMapperFilter;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.DsMapperFixed;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Options.Option;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Options.OptionDouble;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Options.OptionSingle;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Options.OptionToggle;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Options.ParameterInt;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Options.ParameterLong;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Options.ParameterString;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;

/**
 *
 * @author scf
 */
public class OptionsCsv {

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(OptionsCsv.class);
    private Comparator<String> keyComparator = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            if (o1.length() == o2.length()) {
                return String.CASE_INSENSITIVE_ORDER.compare(o1, o2);
            }
            return Integer.compare(o1.length(), o2.length());
        }
    };
    private final Set<String> keys = new HashSet<>();
    private final Map<String, Option> optionMap = new TreeMap<>(keyComparator);
    private final List<Option> options = new ArrayList<>();
    private final OptionSingle<String> charset;
    private final OptionSingle<String> fileName;
    private final OptionSingle<String> serverUrl;
    private final OptionSingle<Integer> colResult;
    private final OptionSingle<Integer> colPhenomenonTime;
    private final OptionSingle<Integer> colValidTime;
    private final OptionSingle<Integer> colResultTime;
    private final OptionSingle<Long> datastreamId;
    private final OptionSingle<String> datastreamFilter;
    private final OptionSingle<Long> rowSkip;
    private final OptionSingle<Long> rowLimit;
    private final OptionSingle<Long> sleep;
    private final OptionToggle noAct;
    private final OptionToggle tabDelimited;
    private final OptionToggle useDataArray;
    private final OptionDouble<String, String> basicAuth;
    private final OptionSingle<Integer> messageInterval;

    public OptionsCsv() {
        charset = addOption(
                new OptionSingle<String>("-charset", "-c")
                .setParam(new ParameterString("character set", "UTF-8"))
                .setDescription("The character set to use when parsing the csv file."));
        fileName = addOption(
                new OptionSingle<String>("-file", "-f")
                .setParam(new ParameterString("file path", ""))
                .setDescription("The path to the csv file."));
        serverUrl = addOption(
                new OptionSingle<String>("-server", "-s")
                .setParam(new ParameterString("url", ""))
                .setDescription("The url of the server."));
        colResult = addOption(
                new OptionSingle<Integer>("-resultCol", "-rc")
                .setParam(new ParameterInt("column nr", -1))
                .setDescription("The column # that holds the result (first is 0)."));
        colPhenomenonTime = addOption(
                new OptionSingle<Integer>("-phenTimeCol", "-ptc")
                .setParam(new ParameterInt("column nr", -1))
                .setDescription("The column # that holds the phenomenonTime (first is 0)."));
        colValidTime = addOption(
                new OptionSingle<Integer>("-validTimeCol", "-vtc")
                .setParam(new ParameterInt("column nr", -1))
                .setDescription("The column # that holds the validTime (first is 0)."));
        colResultTime = addOption(
                new OptionSingle<Integer>("-resultTimeCol", "-rtc")
                .setParam(new ParameterInt("column nr", -1))
                .setDescription("The column # that holds the resultTime (first is 0)."));
        datastreamId = addOption(
                new OptionSingle<Long>("-datastream", "-d")
                .setParam(new ParameterLong("datastream Id", Long.MIN_VALUE))
                .setDescription("The datastream id to add the observations to."));
        datastreamFilter = addOption(
                new OptionSingle<String>("-datastreamFilter", "-dsf")
                .setParam(new ParameterString("filter", ""))
                .setDescription(
                        "A filter that will be added to the query for the datastream.",
                        "Use placeholders {colNr} to add the content of columns to the query.",
                        "Example: -dsf 'Thing/properties/id eq {1}'"));
        rowSkip = addOption(
                new OptionSingle<Long>("-rowSkip", "-rs")
                .setParam(new ParameterLong("row count", 0L))
                .setDescription("The number of rows to skip when reading the file (0=none)."));
        rowLimit = addOption(
                new OptionSingle<Long>("-rowLimit", "-rl")
                .setParam(new ParameterLong("row count", 0L))
                .setDescription("The maximum number of rows to insert as observations (0=no limit)."));
        sleep = addOption(
                new OptionSingle<Long>("-sleep")
                .setParam(new ParameterLong("duration", 0L))
                .setDescription("Sleep for this number of ms after each insert."));
        noAct = addOption(
                new OptionToggle("-noact", "-n")
                .setDescription("Read the file and give output, but do not actually post observations."));
        tabDelimited = addOption(
                new OptionToggle("-tab")
                .setDescription("Use tab as delimiter instead of comma."));
        useDataArray = addOption(
                new OptionToggle("-dataArray", "-da")
                .setDescription("Use the SensorThingsAPI DataArray extension to post Observations.",
                        "This is much more efficient when posting many observations.",
                        "The number of items grouped together is determined by the messageInterval setting."));
        basicAuth = addOption(
                new OptionDouble<String, String>("-basic")
                .setParams(new ParameterString("username", ""), new ParameterString("password", ""))
                .setDescription("Use basic auth."));
        messageInterval = addOption(
                new OptionSingle<Integer>("-messageInterval", "-mi")
                .setParam(new ParameterInt("interval", 10000))
                .setDescription("Output a progress message every [interval] records. Defaults to 10000"));
    }

    public List<Option> getOptions() {
        return options;
    }

    private <T extends Option> T addOption(T o) {
        for (String key : o.getKeys()) {
            if (keys.contains(key)) {
                throw new IllegalStateException("Key " + key + " is defined by more than one option.");
            }
            keys.add(key);
            optionMap.put(key, o);
        }
        options.add(o);
        return o;
    }

    public OptionsCsv parseArguments(List<String> args) {
        List<String> arguments = new ArrayList<>(args);
        while (!arguments.isEmpty()) {
            String key = arguments.get(0);
            if (!key.startsWith("-")) {
                // Not an option.
                LOGGER.debug("Not an option: {}", key);
                continue;
            }

            Option option = optionMap.get(key);
            if (option == null) {
                for (String optionKey : optionMap.keySet()) {
                    if (key.startsWith(optionKey)) {
                        option = optionMap.get(optionKey);
                        break;
                    }
                }
            }

            if (option == null) {
                LOGGER.debug("Unknown option: {}", key);
            } else {
                option.consume(arguments);
            }
        }
        return this;
    }

    public DatastreamMapper createDatastreamMapper(SensorThingsService service) throws ServiceFailureException {
        if (datastreamId.isSet()) {
            Long dsId = datastreamId.getValue();
            if (dsId == Long.MIN_VALUE) {
                throw new IllegalArgumentException("Missing datastream id (-datastream, -d)");
            }
            Datastream datastream = service.datastreams().find(dsId);
            LOGGER.info("Using fixed datatsream: {}", datastream.getName());
            return new DsMapperFixed(datastream);
        }
        if (datastreamFilter.isSet()) {
            return new DsMapperFilter(service, datastreamFilter.getValue());
        }
        LOGGER.error("No way to create a DatastreamMapper (-datastream or -datastreamFilter)");
        throw new IllegalArgumentException("No way to create a DatastreamMapper (-datastream or -datastreamFilter)");
    }

    /**
     * @return the fileName
     */
    public OptionSingle<String> getFileName() {
        return fileName;
    }

    /**
     * @return the serverUrl
     */
    public OptionSingle<String> getServerUrl() {
        return serverUrl;
    }

    /**
     * @return the colResult
     */
    public OptionSingle<Integer> getColResult() {
        return colResult;
    }

    /**
     * @return the colPhenomenonTime
     */
    public OptionSingle<Integer> getColPhenomenonTime() {
        return colPhenomenonTime;
    }

    /**
     * @return the colValidTime
     */
    public OptionSingle<Integer> getColValidTime() {
        return colValidTime;
    }

    /**
     * @return the colResultTime
     */
    public OptionSingle<Integer> getColResultTime() {
        return colResultTime;
    }

    /**
     * @return the datastreamId
     */
    public OptionSingle<Long> getDatastreamId() {
        return datastreamId;
    }

    public OptionToggle getNoAct() {
        return noAct;
    }

    public OptionToggle getTabDelimited() {
        return tabDelimited;
    }

    public OptionToggle getUseDataArray() {
        return useDataArray;
    }

    public OptionSingle<Long> getRowLimit() {
        return rowLimit;
    }

    public OptionSingle<Long> getRowSkip() {
        return rowSkip;
    }

    public OptionSingle<String> getCharset() {
        return charset;
    }

    public OptionSingle<Long> getSleep() {
        return sleep;
    }

    public OptionDouble<String, String> getBasicAuth() {
        return basicAuth;
    }

    public OptionSingle<Integer> getMessageInterval() {
        return messageInterval;
    }

}
