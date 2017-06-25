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
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.DatastreamMapper;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.TimeObject;
import de.fraunhofer.iosb.ilt.sta.model.ext.DataArrayValue;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.time.ZonedDateTime;
import java.util.HashSet;
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
	private int colResult;
	private int colPhenTime;
	private int colResultTime;
	private int colValidTime;
	private DatastreamMapper dsm;

	private EditorMap<SensorThingsService, Object, Map<String, Object>> editor;
	private EditorSubclass<SensorThingsService, Object, DatastreamMapper> editorDsMapper;
	private EditorInt editorColResult;
	private EditorInt editorColPhenTime;
	private EditorInt editorColResultTime;
	private EditorInt editorColValidTime;

	public RecordConverterCSV() {
	}

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx) {
		getConfigEditor(context, edtCtx).setConfig(config, context, edtCtx);

		dsm = editorDsMapper.getValue();
		colResult = editorColResult.getValue();
		colPhenTime = editorColPhenTime.getValue();
		colResultTime = editorColResultTime.getValue();
		colValidTime = editorColValidTime.getValue();
	}

	@Override
	public EditorMap<SensorThingsService, Object, Map<String, Object>> getConfigEditor(SensorThingsService context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorMap<>();

			editorColResult = new EditorInt(0, 99, 1, -1, "Result Column", "The column # that holds the result (first is 0).");
			editor.addOption("colResult", editorColResult, false);

			editorColPhenTime = new EditorInt(0, 99, 1, -1, "PhenomenonTime Column", "The column # that holds the phenomenonTime (first is 0).");
			editor.addOption("colPhenTime", editorColPhenTime, true);

			editorColResultTime = new EditorInt(0, 99, 1, -1, "ResultTime Column", "The column # that holds the resultTime (first is 0).");
			editor.addOption("colResultTime", editorColResultTime, false);

			editorColValidTime = new EditorInt(0, 99, 1, -1, "ValidTime Column", "The column # that holds the validTime (first is 0).");
			editor.addOption("colValidTime", editorColValidTime, false);

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
