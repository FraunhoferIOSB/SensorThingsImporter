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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.csv;

import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.Configurable;
import de.fraunhofer.iosb.ilt.configurable.ConfigurationException;
import de.fraunhofer.iosb.ilt.configurable.EditorFactory;
import de.fraunhofer.iosb.ilt.configurable.Utils;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorClass;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorList;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.DatastreamMapper;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.parsers.Parser;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.parsers.ParserTime;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.JsonUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.TimeObject;
import de.fraunhofer.iosb.ilt.sta.model.ext.DataArrayValue;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.math.BigDecimal;
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
	private ParserTime timeParser;
	private Parser resultParser;
	private String parametersTemplate;

	private EditorMap<Map<String, Object>> editor;
	private EditorSubclass<SensorThingsService, Object, DatastreamMapper> editorDsMapper;
	private EditorList<Integer, EditorInt> editorColsResult;
	private EditorInt editorColPhenTime;
	private EditorInt editorColResultTime;
	private EditorInt editorColValidTime;
	private EditorClass<SensorThingsService, Object, ParserTime> editorTimeParser;
	private EditorSubclass<SensorThingsService, Object, Parser> editorResultParser;
	private EditorString editorParameters;

	public RecordConverterCSV() {
	}

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx, ConfigEditor<?> configEditor) throws ConfigurationException {
		getConfigEditor(context, edtCtx).setConfig(config);

		dsm = editorDsMapper.getValue();
		colsResult = editorColsResult.getValue();
		colPhenTime = editorColPhenTime.getValue();
		colResultTime = editorColResultTime.getValue();
		colValidTime = editorColValidTime.getValue();

		timeParser = editorTimeParser.getValue();
		resultParser = editorResultParser.getValue();
		parametersTemplate = editorParameters.getValue();
	}

	@Override
	public EditorMap<Map<String, Object>> getConfigEditor(SensorThingsService context, Object edtCtx) {
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

			editorDsMapper = new EditorSubclass<>(context, edtCtx, DatastreamMapper.class, "datastream", "Maps the record to a datastream.", false, "className");
			editor.addOption("dsMapper", editorDsMapper, false);

			editorTimeParser = new EditorClass<>(context, edtCtx, ParserTime.class, "Time Parser", "The parser to use for parsing times.");
			editor.addOption("timeParser", editorTimeParser, true);

			editorResultParser = new EditorSubclass<>(context, edtCtx, Parser.class, "Result Parser", "The parser to use for parsing results.");
			editor.addOption("resultParser", editorResultParser, true);

			editorParameters = new EditorString("", 4, "parameters Template", "Template used to generate Observation/parameters");
			editor.addOption("parametersTemplate", editorParameters, true);
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
			int colResult = colsResult.get(0);
			if (colResult >= record.size()) {
				return null;
			}
			String resultString = record.get(colResult);
			result = parseResult(resultString);
			if (result == null) {
				LOGGER.debug("No result found in column {}.", colsResult.get(0));
				return null;
			}
			Datastream datastream = dsm.getDatastreamFor(record);
			obs = new Observation(result, datastream);
			log = new StringBuilder("Result: _").append(result).append("_");
		} else {
			List<Object> resultList = new ArrayList<>();
			boolean allnull = true;
			for (Integer colResult : colsResult) {
				if (colResult >= record.size()) {
					return null;
				}
				String resultString = record.get(colResult);
				Object parsed = parseResult(resultString);
				if (parsed != null) {
					allnull = false;
				}
				resultList.add(parsed);
			}
			result = resultList;
			if (allnull) {
				LOGGER.debug("No results found in columns {}.", colsResult);
				return null;
			}
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
		if (!Utils.isNullOrEmpty(parametersTemplate)) {
			String filledTemplate = Translator.fillTemplate(parametersTemplate, record);
			obs.setParameters(JsonUtils.jsonToMap(filledTemplate));
		}
		if (verbose) {
			LOGGER.info(log.toString());
		}
		LOGGER.trace("Record: {}", record.toString());
		return obs;
	}

	private TimeObject parseTimeObject(String value) throws ImportException {
		try {
			if (timeParser != null) {
				ZonedDateTime zdt = timeParser.parse(value);
				return new TimeObject(zdt);
			}
		} catch (Exception e) {
			// Not a ZonedDateTime
		}
		try {
			return TimeObject.parse(value);
		} catch (Exception e) {
			// Not a timeObject
		}
		try {
			return new TimeObject(parseTimestamp(value));
		} catch (ImportException e) {
			// Not anything we know!
			LOGGER.debug("Failed to parse time.", e);
			throw new ImportException("Time value " + value + " could not be parsed as a time.");
		}
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
			return Integer.parseInt(resultString);
		} catch (NumberFormatException e) {
			LOGGER.trace("Not an Integer.");
		}
		try {
			return Long.parseLong(resultString);
		} catch (NumberFormatException e) {
			LOGGER.trace("Not a long.");
		}
		try {
			return new BigDecimal(resultString);
		} catch (NumberFormatException e) {
			LOGGER.trace("Not a BigDecimal.");
		}
		if (resultString.isEmpty()) {
			return null;
		}
		if ("null".equalsIgnoreCase(resultString)) {
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
