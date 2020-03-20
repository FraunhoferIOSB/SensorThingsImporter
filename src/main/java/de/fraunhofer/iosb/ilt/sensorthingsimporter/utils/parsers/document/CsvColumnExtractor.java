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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.parsers.document;

import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.ConfigurationException;
import de.fraunhofer.iosb.ilt.configurable.EditorFactory;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorList;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.DatastreamMapper;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.parsers.Parser;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 *
 * @author scf
 */
public class CsvColumnExtractor implements DocumentParser {

	private EditorMap<Map<String, Object>> editor;
	private EditorList<DatastreamMapper, EditorSubclass<SensorThingsService, Object, DatastreamMapper>> editorDsMappers;
	private EditorList<Parser, EditorSubclass<SensorThingsService, Object, Parser>> editorParsers;
	private EditorInt editorRowSkip;
	private EditorString editorCharSet;
	private EditorString editorDelimiter;
	private EditorBoolean editorTabDelim;

	private List<Parser> parsers;
	private List<DatastreamMapper> dsms;

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx, ConfigEditor<?> configEditor) throws ConfigurationException {
		getConfigEditor(context, edtCtx).setConfig(config);
		parsers = editorParsers.getValue();
		dsms = editorDsMappers.getValue();
	}

	@Override
	public ConfigEditor<?> getConfigEditor(SensorThingsService context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorMap<>();

			EditorFactory<EditorSubclass<SensorThingsService, Object, DatastreamMapper>> factoryDsm;
			factoryDsm = () -> {
				return new EditorSubclass<>(context, edtCtx, DatastreamMapper.class, "Datastream Mapper", "Mapper that returns the Datastream for one column, or a MultiDatastream for all columns.");
			};
			editorDsMappers = new EditorList<>(factoryDsm, "Datastream Mappers", "A Mapper for each column, or a single mapper for a multiDatastream.");
			editor.addOption("dsMappers", editorDsMappers, false);

			EditorFactory<EditorSubclass<SensorThingsService, Object, Parser>> factoryParser;
			factoryParser = () -> {
				return new EditorSubclass<>(context, edtCtx, Parser.class, "Data Parsers", "A parser to parse string data.");
			};
			editorParsers = new EditorList<>(factoryParser, "Data Parsers", "Parsers the parse the columns. 1 for each column, or a single to use the same for all columns.");
			editor.addOption("parsers", editorParsers, false);

			editorRowSkip = new EditorInt(0, 99999, 1, 0, "Rowskip", "Number of headers to skip.");
			editor.addOption("duration", editorRowSkip, true);

			editorCharSet = new EditorString("UTF-8", 1, "Characterset", "The character set to use when parsing the csv file (default UTF-8).");
			editor.addOption("charset", editorCharSet, true);

			editorDelimiter = new EditorString("", 1, "delimiter", "The delimiter to use instead of comma.");
			editor.addOption("delimiter", editorDelimiter, true);

			editorTabDelim = new EditorBoolean(false, "Tab Delimited", "Use tab as delimiter instead of comma.");
			editor.addOption("tab", editorTabDelim, true);
		}
		return editor;
	}

	private List<Observation> process(String data) throws IOException {
		CSVFormat format = CSVFormat.DEFAULT;
		if (editorTabDelim.getValue()) {
			format = format.withDelimiter('\t');
		}
		String delim = editorDelimiter.getValue();
		if (delim.length() > 0) {
			format = format.withDelimiter(delim.charAt(0));
		}
		CSVParser parser = CSVParser.parse(data, format);

		int rowSkip = editorRowSkip.getValue();
		List<List<Object>> results = new ArrayList<>();
		List<Boolean> hasData = new ArrayList<>();
		for (CSVRecord record : parser) {
			if (rowSkip > 0) {
				rowSkip--;
				continue;
			}
			int column = 0;
			for (String value : record) {
				while (column >= results.size()) {
					results.add(new ArrayList<>());
					hasData.add(Boolean.FALSE);
					if (column >= parsers.size()) {
						parsers.add(parsers.get(parsers.size() - 1));
					}
				}
				List<Object> result = results.get(column);
				if (value.isEmpty()) {
					result.add(null);
				} else {
					Parser resultParser = parsers.get(column);
					result.add(resultParser.parse(value));
					hasData.set(column, Boolean.TRUE);
				}
				column++;
			}
		}
		List<Observation> observations = new ArrayList<>();
		int dataCount = 0;
		for (Boolean colHasData : hasData) {
			if (colHasData) {
				dataCount++;
			}
		}

		MultiDatastream mds;
		List<List<Object>> mdsResult = null;
		if (dsms.size() == 1 && dataCount > 1) {
			mds = dsms.get(0).getMultiDatastreamFor(null);
			mdsResult = new ArrayList<>();
			Observation obs = new Observation(mdsResult, mds);
			observations.add(obs);
		}

		int dataColumn = 0;
		for (int column = 0; column < results.size(); column++) {
			if (hasData.get(column)) {
				if (mdsResult == null) {
					Observation obs = new Observation();
					obs.setResult(results.get(column));
					obs.setDatastream(dsms.get(dataColumn).getDatastreamFor(null));
					dataColumn++;
					observations.add(obs);
				} else {
					mdsResult.add(results.get(column));
				}
			}
		}
		return observations;
	}

	@Override
	public List<Observation> process(Datastream ds, String input) throws ImportException {
		try {
			return process(input);
		} catch (IOException exc) {
			throw new ImportException(exc);
		}
	}

	@Override
	public List<Observation> process(MultiDatastream mds, String... inputs) throws ImportException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

}
