/*
 * Copyright (C) 2026 Fraunhofer Institut IOSB, Fraunhoferstr. 1, D 76131
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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.records;

import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorClass;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.datagen.DataGenerator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ErrorLog;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.InspectingIterator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UrlUtils.HttpResponse;
import de.fraunhofer.iosb.ilt.sta.Utils;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns a CSV file into Tuples.
 */
public class TupleSourceCsv implements TupleSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(TupleSourceCsv.class.getName());

    @ConfigurableField(editor = EditorSubclass.class,
            label = "Input", description = "The input")
    @EditorSubclass.EdOptsSubclass(iface = DataGenerator.class)
    private DataGenerator input;

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

    @ConfigurableField(editor = EditorClass.class, optional = true,
            label = "Error Logger", description = "Configuration of the error logger")
    @EditorClass.EdOptsClass(clazz = ErrorLog.class)
    private ErrorLog errorLog;

    private SensorThingsService service;
    private CSVFormat format;

    @Override
    public void init(SensorThingsService service) throws ImportException {
        this.service = service;

        CSVFormat.Builder formatBuilder = CSVFormat.DEFAULT
                .builder()
                .setDelimiter(tabIsDelimeter ? '\t' : delimiter.charAt(0));
        if (!Utils.isNullOrEmpty(commentMarker)) {
            formatBuilder.setCommentMarker(commentMarker.charAt(0));
        }
        if (hasHeader) {
            formatBuilder.setSkipHeaderRecord(true)
                    .setAllowMissingColumnNames(true);
        }
        format = formatBuilder.get();
    }

    @Override
    public InspectingIterator<Tuple> iterator() {
        return new TupleIterator(this);
    }

    private final class TupleIterator implements InspectingIterator<Tuple> {

        private final InspectingIterator<HttpResponse> dataIterator;
        private Iterator<CSVRecord> records;
        private CSVParser openParser;
        private final boolean limitRows;
        private final long rowLimit;
        private final long rowSkipBase;
        private long rowSkip;
        private int rowCount = 0;
        private int currentLine;

        public TupleIterator(TupleSourceCsv parent) {
            this.rowSkipBase = parent.rowSkip;
            this.rowSkip = parent.rowSkip;
            this.dataIterator = parent.input.items(errorLog).iterator();
            this.openParser = nextData(openParser);
            this.records = openParser.iterator();
            this.rowLimit = parent.rowLimit;
            this.limitRows = parent.rowLimit > 0;
        }

        @Override
        public String getCurrentLocation() {
            return "row " + currentLine + " of " + dataIterator.getCurrentLocation();
        }

        @Override
        public boolean hasNext() {
            if (limitRows && rowCount > rowLimit) {
                return false;
            }
            return records != null && records.hasNext() || dataIterator.hasNext();
        }

        @Override
        public Tuple next() {
            if (!records.hasNext()) {
                try {
                    openParser = nextData(openParser);
                    if (openParser == null) {
                        return null;
                    }
                    records = openParser.iterator();
                    currentLine = 0;
                    errorLog.setCurrentLine(currentLine);
                } catch (RuntimeException ex) {
                    LOGGER.error("Failed to import line {}, URL {}.", currentLine, dataIterator.getCurrentLocation());
                    throw new IllegalStateException(ex);
                }
            }
            while (records.hasNext() && rowSkip > 0) {
                records.next();
                currentLine++;
                errorLog.setCurrentLine(currentLine);
                rowSkip--;
            }

            if (records != null && records.hasNext()) {
                rowCount++;
                currentLine++;
                errorLog.setCurrentLine(currentLine);
                return Translator.CsvTuple.of(records.next(), stripNull);
            }
            return null;
        }

        private CSVParser nextData(CSVParser oldParser) throws ImportException {
            if (oldParser != null)
                try {
                    oldParser.close();
                } catch (IOException ex) {
                    LOGGER.error("Failed to close open parser: {}", ex.getMessage());
                }
            rowSkip = rowSkipBase;
            while (dataIterator.hasNext()) {
                HttpResponse dataResponse = dataIterator.next();
                if (dataResponse == null) {
                    LOGGER.error("No valid input url or file.");
                    return null;
                }
                try {
                    CSVParser parser;
                    errorLog.setCurrentFileName(dataIterator.getCurrentLocation());
                    Reader data = dataResponse.getDataReader();
                    parser = CSVParser.parse(data, format);

                    return parser;
                } catch (ImportException | IOException exc) {
                    LOGGER.error("Failed to handle URL: {}; {}", dataIterator.getCurrentLocation(), exc.getMessage());
                }
            }
            LOGGER.error("NextUrl requested, but no URLs left over.");
            return null;
        }

    }
}
