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
import de.fraunhofer.iosb.ilt.configurable.editor.EditorClass;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.datagen.DataGenerator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ErrorLog;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.InspectingIterator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UrlUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UrlUtils.HttpResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import net.time4j.Moment;
import net.time4j.PlainTimestamp;
import net.time4j.TemporalType;
import net.time4j.tz.ZonalOffset;
import org.apache.commons.io.FileUtils;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleSourceParquet implements TupleSource {

    public static final int MIO = 1_000_000;
    public static final int KILO = 1_000;
    public static final long JULIAN_TO_UNIX_EPOCH_DAYS = 2440588;
    public static final long SECONDS_PER_DAY = 24 * 60 * 60;
    public static final long MILLIS_PER_DAY = SECONDS_PER_DAY * KILO;

    private static final Logger LOGGER = LoggerFactory.getLogger(TupleSourceParquet.class.getName());

    @ConfigurableField(editor = EditorSubclass.class,
            label = "Input", description = "The input")
    @EditorSubclass.EdOptsSubclass(iface = DataGenerator.class)
    private DataGenerator input;

    @ConfigurableField(editor = EditorInt.class, optional = true,
            label = "Row Limit", description = "The maximum number of rows to insert as observations (0=no limit).")
    @EditorInt.EdOptsInt(dflt = 0, max = Integer.MAX_VALUE, min = 0, step = 1)
    private Integer rowLimit;

    @ConfigurableField(editor = EditorClass.class, optional = true,
            label = "Error Logger", description = "Configuration of the error logger")
    @EditorClass.EdOptsClass(clazz = ErrorLog.class)
    private ErrorLog errorLog;

    @Override
    public InspectingIterator<Tuple> iterator() {
        return new TupleIterator(this);
    }

    private final class TupleIterator implements InspectingIterator<Tuple> {

        private final InspectingIterator<HttpResponse> dataIterator;
        private Path tempFile;
        private String currentLocation;
        private ParquetReader<Group> openReader;
        private Group nextRecord;
        private final boolean limitRows;
        private final long rowLimit;
        private int rowCount = 0;
        private int currentLine;

        public TupleIterator(TupleSourceParquet parent) {
            this.dataIterator = parent.input.items(errorLog).iterator();
            openReader = nextData(openReader);
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
            while (nextRecord == null && dataIterator.hasNext()) {
                prepareNextRecord();
                if (nextRecord == null && dataIterator.hasNext()) {
                    LOGGER.debug("Opening new parquet file...");
                    openReader = nextData(openReader);
                    prepareNextRecord();
                }
            }
            return nextRecord != null;
        }

        private void prepareNextRecord() {
            try {
                LOGGER.trace("Reading Record {}", currentLine);
                nextRecord = openReader.read();
                rowCount++;
                currentLine++;
                errorLog.setCurrentLine(currentLine);
            } catch (IOException ex) {
                LOGGER.error("Failed to read record file {} from {}", tempFile, currentLocation);
            } catch (Error exc) {
                LOGGER.error("Failed to read record", exc);
            }
        }

        @Override
        public Tuple next() {
            if (nextRecord == null) {
                hasNext();
            }
            Group returned = nextRecord;
            nextRecord = null;
            return ParquetTuple.of(returned);
        }

        private ParquetReader<Group> nextData(ParquetReader<Group> oldData) throws ImportException {
            if (oldData != null) {
                LOGGER.debug("Finished reading {} records from {} ({})", currentLine, currentLocation, tempFile);
                try {
                    oldData.close();
                } catch (IOException ex) {
                    LOGGER.error("Failed to close reader {}", tempFile, ex);
                }
            }
            currentLine = 0;
            while (dataIterator.hasNext()) {
                if (tempFile != null && Files.exists(tempFile)) {
                    try {
                        Files.delete(tempFile);
                    } catch (IOException ex) {
                        LOGGER.warn("Failed to delete temp file: {}", tempFile);
                    }
                    tempFile = null;
                }
                try (UrlUtils.HttpResponse dataResponse = dataIterator.next()) {
                    currentLocation = dataIterator.getCurrentLocation();
                    if (dataResponse == null) {
                        LOGGER.error("No valid input url or file: {}", currentLocation);
                        continue;
                    }
                    errorLog.setCurrentFileName(currentLocation);
                    tempFile = Files.createTempFile("tsParquet_", ".parquet");
                    LOGGER.debug("Created temp file: {}", tempFile);
                    FileUtils.copyInputStreamToFile(dataResponse.getDataBinary(), tempFile.toFile());
                    LOGGER.debug("Reading file: {}", tempFile);
                    final HadoopParquetConfiguration conf = new HadoopParquetConfiguration();
                    conf.set("parquet.avro.readInt96AsFixed", "true");
                    org.apache.hadoop.fs.Path hPath = new org.apache.hadoop.fs.Path(tempFile.toUri());
                    return ParquetReader.builder(new GroupReadSupport(), hPath)
                            .withConf(conf)
                            .build();
                    // final LocalInputFile localInputFile = new LocalInputFile(tempFile);
                    // return AvroParquetReader.genericRecordReader(new LocalInputFile(tempFile), conf);
                } catch (RuntimeException | IOException exc) {
                    LOGGER.error("Failed to handle URL: {}; {}", currentLocation, exc.getMessage());
                } catch (Error exc) {
                    LOGGER.error("Failed to handle URL: {}", currentLocation, exc);
                }
            }
            LOGGER.error("NextUrl requested, but no URLs left over.");
            return null;
        }
    }

    public static class ParquetTuple implements Tuple {

        private final Group record;

        public ParquetTuple(Group record) {
            this.record = record;
        }

        @Override
        public boolean isMapped(String name) {
            return record.getType().containsField(name);
        }

        @Override
        public String getString(String name) {
            final int idx = record.getType().getFieldIndex(name);
            Type type = record.getType().getType(idx);
            LogicalTypeAnnotation lta = type.getLogicalTypeAnnotation();
            try {
                if (lta == null && type.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
                    final Binary int96 = record.getInt96(idx, 0);
                    final NanoTime nanotime = NanoTime.fromBinary(int96);
                    final int julianDay = nanotime.getJulianDay();
                    final long timeOfDayNanos = nanotime.getTimeOfDayNanos();
                    final long msUnix = MILLIS_PER_DAY * (julianDay - JULIAN_TO_UNIX_EPOCH_DAYS) + timeOfDayNanos / MIO;
                    final Moment m = TemporalType.MILLIS_SINCE_UNIX.translate(msUnix);
                    final PlainTimestamp zt = m.toZonalTimestamp(ZonalOffset.UTC);
                    StringBuilder sb = new StringBuilder(29);
                    sb.append(zt.getCalendarDate().toString());
                    sb.append('T');
                    append2Digits(zt.getHour(), sb);
                    sb.append(':');
                    append2Digits(zt.getMinute(), sb);
                    sb.append(':');
                    append2Digits(zt.getSecond(), sb);

                    if (zt.getNanosecond() != 0) {
                        printNanos(sb, zt.getNanosecond());
                    }

                    return sb.toString();
                }
                if (lta instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
                    return Integer.toString(record.getInteger(idx, 0));
                }
                String stringify = type.asPrimitiveType().stringifier().stringify(record.getBinary(idx, 0));
                return stringify;
            } catch (RuntimeException ex) {
                LOGGER.trace("Failed to fetch.", ex);
                return null;
            }
        }

        @Override
        public String getString(int idx) {
            return record.getString(idx, 0);

        }

        public static ParquetTuple of(Group record) {
            return new ParquetTuple(record);
        }

    }

    public static void append2Digits(int value, StringBuilder sb) {
        if (value < 10) {
            sb.append('0');
        }
        sb.append(value);
    }

    public static void printNanos(StringBuilder sb, int nano) {
        sb.append('.');
        String num = Integer.toString(nano);
        int len;

        if ((nano % MIO) == 0) {
            len = 3;
        } else if ((nano % KILO) == 0) {
            len = 6;
        } else {
            len = 9;
        }

        for (int i = num.length(); i < 9; i++) {
            sb.append('0');
        }

        for (int i = 0, n = len + num.length() - 9; i < n; i++) {
            sb.append(num.charAt(i));
        }

    }

}
