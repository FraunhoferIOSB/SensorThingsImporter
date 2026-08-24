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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.importers;

import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorClass;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorList;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Importer;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.records.RecordConverter;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.records.Tuple;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.records.TupleSource;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ErrorLog;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.InspectingIterator;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An importer that imports Tuples from a database, CSV file, or similar source.
 */
public class ImporterTuples implements Importer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ImporterTuples.class.getName());

    @ConfigurableField(editor = EditorList.class,
            label = "Converters", description = "The classes that convert columns into observations.")
    @EditorList.EdOptsList(editor = EditorSubclass.class, minCount = 1, labelText = "Add a Converter")
    @EditorSubclass.EdOptsSubclass(iface = RecordConverter.class)
    private List<RecordConverter> recordConvertors;

    @ConfigurableField(editor = EditorSubclass.class,
            label = "Source", description = "The source of the tuples to convert.")
    @EditorSubclass.EdOptsSubclass(
            iface = TupleSource.class,
            shortenClassNames = true)
    private TupleSource tupleSource;

    @ConfigurableField(editor = EditorClass.class, optional = true,
            label = "Error Logger", description = "Configuration of the error logger")
    @EditorClass.EdOptsClass(clazz = ErrorLog.class)
    private ErrorLog errorLog;

    @Override
    public void init(SensorThingsService service) throws ImportException {
        for (var rc : recordConvertors) {
            rc.init(service);
        }
        tupleSource.init(service);
    }

    ErrorLog getErrors() {
        return errorLog;
    }

    @Override
    public String getErrorLog() {
        return errorLog.getErrors();
    }

    @Override
    public int getErrorCount() {
        return errorLog.getErrorCount();
    }

    @Override
    public Iterator<List<Observation>> iterator() {
        return new ObsListIter(this);
    }

    public List<RecordConverter> getRecordConvertors() {
        return recordConvertors;
    }

    private static class ObsListIter implements Iterator<List<Observation>> {

        private final ImporterTuples parent;
        private final InspectingIterator<Tuple> tupleIter;
        private final List<RecordConverter> rcvs = new ArrayList<>();

        public ObsListIter(ImporterTuples parent) {
            this.parent = parent;
            tupleIter = parent.tupleSource.iterator();
            rcvs.addAll(parent.getRecordConvertors());
        }

        @Override
        public boolean hasNext() {
            return tupleIter.hasNext();
        }

        @Override
        public List<Observation> next() {
            if (!tupleIter.hasNext()) {
                return Collections.emptyList();
            }
            Tuple nextTuple = tupleIter.next();
            List<Observation> result = new ArrayList<>();

            for (RecordConverter rc : rcvs) {
                List<Observation> obs;
                try {
                    obs = rc.convert(nextTuple, parent.getErrors());
                    result.addAll(obs);
                } catch (ImportException ex) {
                    LOGGER.debug("Failed to import at {}", tupleIter.getCurrentLocation(), ex);
                } catch (RuntimeException ex) {
                    LOGGER.error("Failed to import line {}.", tupleIter.getCurrentLocation(), ex);
                    throw new IllegalStateException(ex);
                }
            }
            return result;
        }

    }

}
