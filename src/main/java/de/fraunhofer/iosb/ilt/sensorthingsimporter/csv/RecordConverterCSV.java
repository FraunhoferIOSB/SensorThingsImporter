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

import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.Configurable;
import de.fraunhofer.iosb.ilt.configurable.EditorFactory;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorList;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.DatastreamMapper;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.TimeObject;
import de.fraunhofer.iosb.ilt.sta.model.ext.DataArrayValue;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

/**
 *
 * @author scf
 */
public class RecordConverterCSV implements Configurable<SensorThingsService, Object> {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(RecordConverterCSV.class);
	private boolean verbose = false;
	private List<Integer> colsResult;
	private int colPhenTime;
	private int colResultTime;
	private int colValidTime;
	private DatastreamMapper dsm;

	private EditorMap<SensorThingsService, Object, Map<String, Object>> editor;
	private EditorSubclass<SensorThingsService, Object, DatastreamMapper> editorDsMapper;
	private EditorList<Object, Object, Integer, EditorInt> editorColsResult;
	private EditorInt editorColPhenTime;
	private EditorInt editorColResultTime;
	private EditorInt editorColValidTime;

	public RecordConverterCSV() {
	}

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx) {
		getConfigEditor(context, edtCtx).setConfig(config, context, edtCtx);

		dsm = editorDsMapper.getValue();
		colsResult = editorColsResult.getValue();
		colPhenTime = editorColPhenTime.getValue();
		colResultTime = editorColResultTime.getValue();
		colValidTime = editorColValidTime.getValue();
	}

	@Override
	public EditorMap<SensorThingsService, Object, Map<String, Object>> getConfigEditor(SensorThingsService context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorMap<>();

			EditorFactory<EditorInt> factory;
			factory = () -> new EditorInt(0, 99, 1, -1, "Result Column", "The column # that holds the result (first is 0).");
			editorColsResult = new EditorList(factory, "result columns", "Only one for normal datastreams, any number for Multidatastreams.");
			editor.addOption("colsResult", editorColsResult, false);

			editorColPhenTime = new EditorInt(0, 99, 1, -1, "PhenomenonTime Column", "The column # that holds the phenomenonTime (first is 0).");
			editor.addOption("colPhenTime", editorColPhenTime, false);

			editorColResultTime = new EditorInt(0, 99, 1, -1, "ResultTime Column", "The column # that holds the resultTime (first is 0).");
			editor.addOption("colResultTime", editorColResultTime, true);

			editorColValidTime = new EditorInt(0, 99, 1, -1, "ValidTime Column", "The column # that holds the validTime (first is 0).");
			editor.addOption("colValidTime", editorColValidTime, true);

			editorDsMapper = new EditorSubclass<>(DatastreamMapper.class, "datastream", "Maps the record to a datastream.", false, "className");
			editor.addOption("dsMapper", editorDsMapper, false);

		}
		return editor;
	}

	public Set<DataArrayValue.Property> getDefinedProperties() {
		Set<DataArrayValue.Property> value = new HashSet<>();
		value.add(DataArrayValue.Property.Result);
		if (colPhenTime >= 0) {
			value.add(DataArrayValue.Property.PhenomenonTime);
		}
		if (colResultTime >= 0) {
			value.add(DataArrayValue.Property.ResultTime);
		}
		if (colValidTime >= 0) {
			value.add(DataArrayValue.Property.ValidTime);
		}
		return value;
	}

	public RecordConverterCSV setVerbose(boolean verbose) {
		this.verbose = verbose;
		return this;
	}

	public Observation convert(CSVRecord record) throws ImportException {
		Object result;
		Observation obs;
		StringBuilder log;
		if (colsResult.isEmpty()) {
			throw new IllegalArgumentException("Must have at least one result column.");
		} else if (colsResult.size() == 1) {
			String resultString = record.get(colsResult.get(0));
			result = parseResult(resultString);
			Datastream datastream = dsm.getDatastreamFor(record);
			obs = new Observation(result, datastream);
			log = new StringBuilder("Result: _").append(result).append("_");
		} else {
			List<Object> resultList = new ArrayList<>();
			for (Integer colResult : colsResult) {
				String resultString = record.get(colResult);
				resultList.add(parseResult(resultString));
			}
			result = resultList;
			MultiDatastream mds = dsm.getMultiDatastreamFor(record);
			obs = new Observation(result, mds);
			log = new StringBuilder("Result: _").append(result).append("_");
		}

		if (colPhenTime >= 0) {
			obs.setPhenomenonTime(parseTimeObject(record.get(colPhenTime)));
			log.append(", phenomenonTime: ").append(obs.getPhenomenonTime());
		}
		if (colResultTime >= 0) {
			obs.setResultTime(parseZonedDateTime(record.get(colResultTime)));
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

	private TimeObject parseTimeObject(String value) throws ImportException {
		try {
			return TimeObject.parse(value);
		} catch (Exception e) {
			// Not a timeObject
		}
		return new TimeObject(parseTimestamp(value));
	}

	private ZonedDateTime parseZonedDateTime(String value) throws ImportException {
		try {
			return ZonedDateTime.parse(value);
		} catch (Exception e) {
			// Not a ZonedDateTime
		}
		return parseTimestamp(value);
	}

	private ZonedDateTime parseTimestamp(String value) throws ImportException {
		try {
			long longValue = Long.parseLong(value);
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(1000 * longValue);
			return ZonedDateTime.ofInstant(cal.toInstant(), ZoneId.systemDefault());
		} catch (NumberFormatException e) {
			throw new ImportException("Could not parse time value " + value);
		}
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
			LOGGER.trace("Not a double.");
		}
		if (resultString.isEmpty()) {
			return null;
		}

		if (resultString.endsWith("%")) {
			Object testResult = parseResult(resultString.substring(0, resultString.length() - 1));
			if (testResult instanceof Double || testResult instanceof Long) {
				return testResult;
			}
		}

		return resultString;
	}

}
