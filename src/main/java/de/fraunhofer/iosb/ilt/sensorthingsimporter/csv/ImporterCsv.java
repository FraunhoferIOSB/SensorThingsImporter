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
import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.ConfigurationException;
import de.fraunhofer.iosb.ilt.configurable.Utils;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorList;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Importer;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UrlUtils;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class ImporterCsv implements Importer, AnnotatedConfigurable<SensorThingsService, Object> {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ImporterCsv.class);
	private SensorThingsService service;
	private boolean verbose;

	private final List<RecordConverter> recordConverters = new ArrayList<>();

	@ConfigurableField(editor = EditorList.class,
			label = "Converters", description = "The classes that convert columns into observations.")
	@EditorList.EdOptsList(editor = EditorSubclass.class, minCount = 1, labelText = "Add a Converter")
	@EditorSubclass.EdOptsSubclass(iface = RecordConverter.class)
	private List<RecordConverter> recordConvertors;

	@ConfigurableField(editor = EditorInt.class, optional = true,
			label = "Row Limit", description = "The maximum number of rows to insert as observations (0=no limit).")
	@EditorInt.EdOptsInt(dflt = 0, max = Integer.MAX_VALUE, min = 0, step = 1)
	private Integer rowLimit;

	@ConfigurableField(editor = EditorInt.class, optional = true,
			label = "Row Skip", description = "The number of rows to skip when reading the file (0=none).")
	@EditorInt.EdOptsInt(dflt = 0, max = Integer.MAX_VALUE, min = 0, step = 1)
	private Integer rowSkip;

	@ConfigurableField(editor = EditorString.class, optional = true,
			label = "Characterset", description = "The character set to use when parsing the csv file (default UTF-8).")
	@EditorString.EdOptsString(dflt = "UTF-8")
	private String charset;

	@ConfigurableField(editor = EditorSubclass.class,
			label = "Input Url", description = "The input url(s)")
	@EditorSubclass.EdOptsSubclass(iface = UrlGenerator.class)
	private UrlGenerator inputUrl;

	@ConfigurableField(editor = EditorString.class, optional = true,
			label = "Delimiter", description = "The character to use as delimiter ('\\t' for tab, default ',').")
	@EditorString.EdOptsString(dflt = ",")
	private String delimiter;

	@ConfigurableField(editor = EditorString.class, optional = true,
			label = "Comment Marker", description = "Lines starting with this character are ignored.")
	@EditorString.EdOptsString(dflt = "")
	private String commentMarker;

	@ConfigurableField(editor = EditorBoolean.class, optional = true,
			label = "Tab Delimited", description = "Is the TAB character a delimeter?")
	@EditorBoolean.EdOptsBool()
	private boolean tabIsDelimeter;

	@ConfigurableField(editor = EditorBoolean.class, optional = true,
			label = "Has Header", description = "Check if the CSV file has a header line.")
	@EditorBoolean.EdOptsBool()
	private boolean hasHeader;

	@ConfigurableField(editor = EditorBoolean.class, optional = true,
			label = "Strip UTF-8 null", description = "Strip UTF-8 null characters.")
	@EditorBoolean.EdOptsBool(dflt = true)
	private boolean stripNull;

	private CSVFormat format;

	public ImporterCsv() {
	}

	@Override
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	@Override
	public void setNoAct(boolean noAct) {
		// Nothing to set.
	}

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx, ConfigEditor<?> configEditor) throws ConfigurationException {
		service = context;
		AnnotatedConfigurable.super.configure(config, context, edtCtx, configEditor);
	}

	private void init() throws ImportException, ConfigurationException {

		recordConverters.clear();
		recordConverters.addAll(recordConvertors);
		for (RecordConverter rcCsv : recordConverters) {
			rcCsv.setVerbose(verbose);
			rcCsv.init(service);
		}

		format = CSVFormat.DEFAULT
				.withDelimiter(delimiter.charAt(0));
		if (tabIsDelimeter) {
			format = format.withDelimiter('\t');
		}
		if (!Utils.isNullOrEmpty(commentMarker)) {
			format = format.withCommentMarker(commentMarker.charAt(0));
		}
		if (hasHeader) {
			format = format.withFirstRecordAsHeader()
					.withAllowMissingColumnNames();
		}
	}

	@Override
	public Iterator<List<Observation>> iterator() {
		try {
			init();
			ObsListIter obsListIter = new ObsListIter(inputUrl.iterator(), rowSkip, rowLimit);
			return obsListIter;
		} catch (ImportException | ConfigurationException exc) {
			throw new IllegalStateException("Failed to handle csv file.", exc);
		}
	}

	private class ObsListIter implements Iterator<List<Observation>> {

		private final Iterator<URL> urlIterator;
		private Iterator<CSVRecord> records;
		private final boolean limitRows;
		private final long rowLimit;
		private final long rowSkipBase;
		private long rowSkip;
		private int rowCount = 0;
		private int totalCount = 0;
		private int currentLine;
		private String currentUrl;

		public ObsListIter(Iterator<URL> urlIterator, long rowSkip, long rowLimit) throws ImportException {
			this.rowSkipBase = rowSkip;
			this.rowSkip = rowSkip;
			this.urlIterator = urlIterator;
			this.records = nextUrl().iterator();
			this.rowLimit = rowLimit;
			limitRows = rowLimit > 0;
		}

		@Override
		public boolean hasNext() {
			return records != null && records.hasNext() || urlIterator.hasNext();
		}

		@Override
		public List<Observation> next() {
			if (!records.hasNext()) {
				try {
					records = nextUrl().iterator();
					currentLine = 0;
				} catch (RuntimeException | ImportException ex) {
					LOGGER.error("Failed to import line {}, URL {}.", currentLine, currentUrl);
					throw new IllegalStateException(ex);
				}
			}
			while (records != null && records.hasNext()) {
				CSVRecord record = records.next();
				totalCount++;
				currentLine++;
				if (rowSkip > 0) {
					rowSkip--;
					continue;
				}
				if (limitRows && rowCount > rowLimit) {
					return Collections.emptyList();
				}
				List<Observation> result = new ArrayList<>();
				for (RecordConverter rcCsv : recordConverters) {
					List<Observation> obs;
					try {
						obs = rcCsv.convert(record);
						result.addAll(obs);
					} catch (ImportException ex) {
						LOGGER.debug("Failed to import line {}, URL {}.", currentLine, currentUrl, ex);
					} catch (RuntimeException ex) {
						LOGGER.error("Failed to import line {}, URL {}.", currentLine, currentUrl, ex);
						throw new IllegalStateException(ex);
					}
				}
				rowCount++;
				return result;
			}
			LOGGER.info("Parsed {} rows of {}.", rowCount, totalCount);
			return Collections.emptyList();
		}

		private CSVParser nextUrl() throws ImportException {
			rowSkip = rowSkipBase;
			while (urlIterator.hasNext()) {
				URL inUrl = urlIterator.next();
				LOGGER.debug("Next URL: {}", inUrl);
				try {
					CSVParser parser;
					if (inUrl != null) {
						currentUrl = inUrl.toString();
						final String protocol = inUrl.getProtocol();
						String data;
						if (protocol.startsWith("ftp")) {
							URLConnection connection = inUrl.openConnection();
							try (InputStream stream = connection.getInputStream()) {
								data = IOUtils.toString(stream, "UTF-8");
							}
						} else {
							data = UrlUtils.fetchFromUrl(inUrl.toString(), charset).data;
						}
						if (stripNull) {
							data = StringUtils.replaceChars(data, "\u0000", "");
						}
						parser = CSVParser.parse(data, format);
					} else {
						LOGGER.error("No valid input url or file.");
						throw new ImportException("No valid input url or file.");
					}
					return parser;
				} catch (ImportException | IOException exc) {
					LOGGER.error("Failed to handle URL: {}; {}", inUrl, exc.getMessage());
				}
			}
			LOGGER.error("NextUrl requested, but no URLs left over.");
			return null;
		}
	}

}
