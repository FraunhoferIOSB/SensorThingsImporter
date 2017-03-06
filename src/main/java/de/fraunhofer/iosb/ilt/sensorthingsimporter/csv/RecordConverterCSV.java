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

import de.fraunhofer.iosb.ilt.sensorthingsimporter.DatastreamMapper;

import java.time.ZonedDateTime;

import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.TimeObject;

/**
 *
 * @author scf
 */
public class RecordConverterCSV {

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordConverterCSV.class);
    private boolean verbose = false;
    private final int colResult;
    private final int colPhenTime;
    private final int colResultTime;
    private final int colValidTime;
    private final DatastreamMapper dsm;

    public RecordConverterCSV(OptionsCsv options, DatastreamMapper datastreamMapper) {
        this.dsm = datastreamMapper;
        colResult = options.getColResult().getValue();
        colPhenTime = options.getColPhenomenonTime().getValue();
        colResultTime = options.getColResultTime().getValue();
        colValidTime = options.getColValidTime().getValue();
    }

    public RecordConverterCSV setVerbose(boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    public Observation convert(CSVRecord record) {
        String resultString = record.get(colResult);
        Object result = parseResult(resultString);
        Datastream datastream = dsm.getDatastreamFor(record);
        Observation obs = new Observation(result, datastream);
        StringBuilder log = new StringBuilder("Result: _").append(result).append("_");

        if (colPhenTime >= 0) {
            obs.setPhenomenonTime(TimeObject.parse(record.get(colPhenTime)));
            log.append(", phenomenonTime: ").append(obs.getPhenomenonTime());
        }
        if (colResultTime >= 0) {
            obs.setResultTime(ZonedDateTime.parse(record.get(colResultTime)));
            log.append(", resultTime: ").append(obs.getResultTime());
        }
        if (colValidTime >= 0) {
            obs.setValidTime(Interval.parse(record.get(colValidTime)));
            log.append(", validTime: ").append(obs.getValidTime());
        }
        if (verbose) {
            LOGGER.info(log.toString());
        }
        LOGGER.trace("Record: {}", record.toString());
        return obs;
    }

    private Object parseResult(String resultString) {
        try {
            return Long.parseLong(resultString);
        } catch (NumberFormatException e) {
            LOGGER.trace("Not a long.");
        }
        try {
            return Double.parseDouble(resultString);
        } catch (NumberFormatException e) {
            LOGGER.trace("Not a long.");
        }
        return resultString;
    }
}
