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

import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.Utils;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorClass;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorList;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.JsonUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UnitConverter;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.parsers.Parser;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.parsers.ParserTime;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.TimeObject;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

/**
 *
 * @author scf
 */
public class RecordConverterDefault implements RecordConverter, AnnotatedConfigurable<SensorThingsService, Object> {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(RecordConverterDefault.class);
	private boolean verbose = false;

	@ConfigurableField(editor = EditorInt.class,
			label = "Result Col", description = "The column # that holds the result (first is 0).")
	@EditorInt.EdOptsInt(dflt = -1, min = -1, max = 99, step = 1)
	private Integer colResult;

	@ConfigurableField(editor = EditorInt.class, optional = true,
			label = "Unit Col", description = "The column # that holds the unit of measurement (first is 0).")
	@EditorInt.EdOptsInt(dflt = -1, min = -1, max = 99, step = 1)
	private Integer colUnit;

	@ConfigurableField(editor = EditorClass.class, optional = true,
			label = "UnitConverter", description = "The converter used to convert units.")
	@EditorClass.EdOptsClass(clazz = UnitConverter.class)
	private UnitConverter converter;

	@ConfigurableField(editor = EditorList.class,
			label = "PhenomenonTime Col", description = "The column(s) # that holds the phenomenonTime (first is 0).")
	@EditorList.EdOptsList(editor = EditorInt.class, minCount = 1, maxCount = 2)
	@EditorInt.EdOptsInt(dflt = -1, min = -1, max = 99, step = 1)
	private List<Integer> colPhenTime;

	@ConfigurableField(editor = EditorInt.class, optional = true,
			label = "ResultTime Column", description = "The column # that holds the resultTime (first is 0).")
	@EditorInt.EdOptsInt(dflt = -1, min = -1, max = 99, step = 1)
	private int colResultTime;

	@ConfigurableField(editor = EditorList.class, optional = true,
			label = "ValidTime Column", description = "The column(s) # that holds the validTime (first is 0).")
	@EditorList.EdOptsList(editor = EditorInt.class, minCount = 0, maxCount = 2)
	@EditorInt.EdOptsInt(dflt = -1, min = -1, max = 99, step = 1)
	private List<Integer> colValidTime;

	@ConfigurableField(editor = EditorSubclass.class,
			label = "Datastream", description = "Maps the record to a datastream.")
	@EditorSubclass.EdOptsSubclass(iface = DatastreamMapper.class)
	private DatastreamMapper dsm;

	@ConfigurableField(editor = EditorClass.class, optional = true,
			label = "Time Parser", description = "The parser to use for parsing times.")
	@EditorClass.EdOptsClass(clazz = ParserTime.class)
	private ParserTime timeParser;

	@ConfigurableField(editor = EditorSubclass.class, optional = true,
			label = "Result Parser", description = "The parser to use for parsing results.")
	@EditorSubclass.EdOptsSubclass(iface = Parser.class)
	private Parser resultParser;

	@ConfigurableField(editor = EditorString.class, optional = true,
			label = "parameters Template", description = "Template used to generate Observation/parameters, using {nr} placeholders.")
	@EditorString.EdOptsString(lines = 4)
	private String parametersTemplate;

	public RecordConverterDefault() {
	}

	@Override
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	@Override
	public List<Observation> convert(CSVRecord record) throws ImportException {
		Object result;
		Observation obs;
		StringBuilder log;
		if (colResult >= record.size()) {
			return Collections.emptyList();
		}
		String resultString = record.get(colResult);
		result = parseResult(resultString);
		if (result == null) {
			LOGGER.debug("No result found in column {}.", colResult);
			return Collections.emptyList();
		}

		Datastream datastream = dsm.getDatastreamFor(record);
		if (datastream == null) {
			LOGGER.debug("No datastream found for column {}", record);
			return Collections.emptyList();
		}
		if (colUnit >= 0) {
			String unitFrom = record.get(colUnit);
			String unitTo = datastream.getUnitOfMeasurement().getSymbol();
			result = convertResult(unitFrom, unitTo, result);
			if (result == null) {
				LOGGER.error("Failed to convert from {} to {}.", unitFrom, unitTo);
				return Collections.emptyList();
			}
		}
		obs = new Observation(result, datastream);
		log = new StringBuilder("Result: _").append(result).append("_");

		obs.setPhenomenonTime(listToTimeObject(colPhenTime, record));
		log.append(", phenomenonTime: ").append(obs.getPhenomenonTime());

		if (colResultTime >= 0) {
			obs.setResultTime(parseZonedDateTime(record.get(colResultTime)));
			log.append(", resultTime: ").append(obs.getResultTime());
		}
		if (!colValidTime.isEmpty()) {
			obs.setValidTime(listToTimeObject(colValidTime, record).getAsInterval());
			log.append(", validTime: ").append(obs.getValidTime());
		}
		if (!Utils.isNullOrEmpty(parametersTemplate)) {
			String filledTemplate = Translator.fillTemplate(parametersTemplate, record, false, true, false);
			obs.setParameters(JsonUtils.jsonToMap(filledTemplate));
		}
		if (verbose) {
			LOGGER.debug(log.toString());
		}
		LOGGER.trace("Record: {}", record);
		return Arrays.asList(obs);
	}

	private Object convertResult(String unitFrom, String unitTo, Object result) {
		if (unitFrom.equals(unitTo)) {
			return result;
		}
		if (converter == null) {
			LOGGER.warn("Do not know how to convert {} to {}.", unitFrom, unitTo);
			return null;
		}
		if (result instanceof BigDecimal) {
			return converter.convert(unitFrom, unitTo, (BigDecimal) result);
		}
		if (result instanceof Number) {
			return converter.convert(unitFrom, unitTo, new BigDecimal(result.toString()));
		}
		return null;
	}

	private TimeObject listToTimeObject(List<Integer> colList, CSVRecord record) throws ImportException {
		if (colList.size() == 2) {
			String start = record.get(colList.get(0));
			String end = record.get(colList.get(1));
			ZonedDateTime startTime = parseTime(start);
			ZonedDateTime endTime = parseTime(end);
			Interval interval = Interval.of(startTime.toInstant(), endTime.toInstant());
			return new TimeObject(interval);
		} else {
			return new TimeObject(parseTime(record.get(colList.get(0))));
		}
	}

	private ZonedDateTime parseTime(String value) throws ImportException {
		try {
			if (timeParser != null) {
				ZonedDateTime zdt = timeParser.parse(value);
				return zdt;
			}
		} catch (Exception e) {
			LOGGER.debug("Failed to parse time using configured timeParser.", e);
			// Not a ZonedDateTime
		}
		try {
			return ZonedDateTime.parse(value);
		} catch (Exception e) {
			// Not a timeObject
		}
		try {
			return parseTimestamp(value);
		} catch (ImportException e) {
			// Not anything we know!
			LOGGER.debug("Failed to parse {} to a time: {}", value, e.getMessage());
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
			LOGGER.debug("Failed to parse {} to a time: {}", value, e.getMessage());
			throw new ImportException("Could not parse time value " + value);
		}
	}

	private Object parseResult(String resultString) {
		if (resultParser != null) {
			return resultParser.parse(resultString);
		}
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
