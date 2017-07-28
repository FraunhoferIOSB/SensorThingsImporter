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

import com.google.common.collect.AbstractIterator;
import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.EditorFactory;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorClass;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorList;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Importer;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class ImporterCsv implements Importer {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ImporterCsv.class);
	private SensorThingsService service;
	private long rowLimit;
	private long rowSkip;
	private boolean verbose;

	private final List<RecordConverterCSV> rcCsvs = new ArrayList<>();

	private EditorMap<Map<String, Object>> editor;
	private EditorBoolean editorUseDataArray;

	private EditorList<RecordConverterCSV, EditorClass<SensorThingsService, Object, RecordConverterCSV>> editorConverters;

	private EditorInt editorRowLimit;
	private EditorInt editorRowSkip;

	private EditorString editorCharSet;
	private EditorString editorInput;
	private EditorString editorDelimiter;
	private EditorBoolean editorTabDelim;

	public ImporterCsv() {
	}

	@Override
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx) {
		service = context;
		getConfigEditor(service, edtCtx).setConfig(config);
	}

	@Override
	public EditorMap<Map<String, Object>> getConfigEditor(final SensorThingsService context, final Object edtCtx) {
		if (editor == null) {
			editor = new EditorMap<>();

			EditorFactory<EditorClass<SensorThingsService, Object, RecordConverterCSV>> factory;
			factory = () -> new EditorClass<>(context, edtCtx, RecordConverterCSV.class);
			editorConverters = new EditorList(factory, "Converters", "The classes that convert columns into observations.");
			editor.addOption("recordConvertors", editorConverters, false);

			editorUseDataArray = new EditorBoolean(false, "Use DataArrays",
					"Use the SensorThingsAPI DataArray extension to post Observations. "
					+ "This is much more efficient when posting many observations. "
					+ "The number of items grouped together is determined by the messageInterval setting.");
			editor.addOption("useDataArrays", editorUseDataArray, true);

			editorRowLimit = new EditorInt(0, Integer.MAX_VALUE, 1, 0, "Row Limit", "The maximum number of rows to insert as observations (0=no limit).");
			editor.addOption("rowLimit", editorRowLimit, true);

			editorRowSkip = new EditorInt(0, Integer.MAX_VALUE, 1, 0, "Row Skip", "The number of rows to skip when reading the file (0=none).");
			editor.addOption("rowSkip", editorRowSkip, true);

			editorCharSet = new EditorString("UTF-8", 1, "Characterset", "The character set to use when parsing the csv file (default UTF-8).");
			editor.addOption("charset", editorCharSet, true);

			editorInput = new EditorString("", 1, "InputUrl", "The path or URL to the csv file.");
			editor.addOption("input", editorInput, false);

			editorDelimiter = new EditorString("", 1, "delimiter", "The delimiter to use instead of comma.");
			editor.addOption("delimiter", editorDelimiter, true);

			editorTabDelim = new EditorBoolean(false, "Tab Delimited", "Use tab as delimiter instead of comma.");
			editor.addOption("tab", editorTabDelim, true);
		}
		return editor;
	}

	public CSVParser init() throws ImportException {

		rcCsvs.clear();
		rcCsvs.addAll(editorConverters.getValue());
		for (RecordConverterCSV rcCsv : rcCsvs) {
			rcCsv.setVerbose(verbose);
		}

		rowLimit = editorRowLimit.getValue();
		rowSkip = editorRowSkip.getValue();

		CSVFormat format = CSVFormat.DEFAULT;
		if (editorTabDelim.getValue()) {
			format = format.withDelimiter('\t');
		}
		String delim = editorDelimiter.getValue();
		if (delim.length() > 0) {
			format = format.withDelimiter(delim.charAt(0));
		}

		String input = editorInput.getValue();
		URI inUrl = null;
		try {
			inUrl = new URI(input);
		} catch (URISyntaxException e) {
		}
		File inFile = null;
		try {
			inFile = new File(input);
		} catch (Exception e) {
		}

		String charset = editorCharSet.getValue();
		CSVParser parser;
		try {
			if (inUrl != null) {
				CloseableHttpClient client = HttpClients.createSystem();
				HttpGet get = new HttpGet(inUrl);
				CloseableHttpResponse response = client.execute(get);
				String data = EntityUtils.toString(response.getEntity(), charset);
				parser = CSVParser.parse(data, format);
			} else if (inFile != null) {
				parser = CSVParser.parse(inFile, Charset.forName(charset), format);
			} else {
				LOGGER.error("Failed");
				throw new ImportException("No valid input url or file.");
			}
		} catch (IOException exc) {
			LOGGER.error("Failed", exc);
			throw new ImportException("Failed to handle csv file.", exc);
		}

		return parser;
	}

	@Override
	public Iterator<List<Observation>> iterator() {

		try {
			final CSVParser parser = init();
			final Iterator<CSVRecord> records = parser.iterator();
			ObsListIter obsListIter = new ObsListIter(records, rowSkip, rowLimit);
			return obsListIter;
		} catch (ImportException exc) {
			LOGGER.error("Failed", exc);
			throw new IllegalStateException("Failed to handle csv file.", exc);
		}
	}

	private class ObsListIter extends AbstractIterator<List<Observation>> {

		private final Iterator<CSVRecord> records;
		private boolean limitRows;
		private long rowLimit;
		private long rowSkip;
		private int rowCount = 0;
		private int totalCount = 0;

		public ObsListIter(Iterator<CSVRecord> records, long rowSkip, long rowLimit) {
			this.rowSkip = rowSkip;
			this.records = records;
			this.rowLimit = rowLimit;
			limitRows = rowLimit > 0;
		}

		@Override
		protected List<Observation> computeNext() {
			while (records.hasNext()) {
				CSVRecord record = records.next();
				totalCount++;
				if (rowSkip > 0) {
					rowSkip--;
					continue;
				}
				if (limitRows && rowCount > rowLimit) {
					return endOfData();
				}
				List<Observation> result = new ArrayList<>();
				for (RecordConverterCSV rcCsv : rcCsvs) {
					Observation obs;
					try {
						obs = rcCsv.convert(record);
						if (obs != null) {
							result.add(obs);
						}
					} catch (ImportException ex) {
						LOGGER.error("Failed to import.", ex);
					}
				}
				rowCount++;
				return result;
			}
			LOGGER.info("Parsed {} rows of {}.", rowCount, totalCount);
			return endOfData();
		}
	}

}
