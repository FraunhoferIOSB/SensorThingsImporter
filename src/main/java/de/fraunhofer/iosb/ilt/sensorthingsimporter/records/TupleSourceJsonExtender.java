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
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.frostclient.SensorThingsService;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.datagen.DataGeneratorFixed;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ErrorLog;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.InspectingIterator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.node.ObjectNode;

/**
 * Loads JSON Tuples from a child TupleSourceJson and extends each tuple.
 *
 * []/timeseries/[]
 */
public class TupleSourceJsonExtender implements TupleSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(TupleSourceJsonExtender.class.getName());

    @ConfigurableField(editor = EditorSubclass.class,
            label = "Main Input", description = "The Main tuple input")
    @EditorSubclass.EdOptsSubclass(iface = TupleSource.class)
    private TupleSource main;

    @ConfigurableField(editor = EditorString.class,
            label = "UrlTemplate", description = "The template for the URL to load additional JSON data.")
    @EditorString.EdOptsString()
    private String baseUrlTemplate;

    @ConfigurableField(editor = EditorString.class,
            label = "Path", description = "The path in the sub JSON document, using [] for arrays that should be iterated over.")
    @EditorString.EdOptsString(dflt = "[]/timeseries/[]")
    private String path = "[]/timeseries/[]";

    @ConfigurableField(editor = EditorString.class,
            label = "ExtensionPath", description = "The path in the main tuple to add the sub data to.")
    @EditorString.EdOptsString(dflt = "station")
    private String extensionPath;

    @ConfigurableField(editor = EditorClass.class, optional = true,
            label = "Error Logger", description = "Configuration of the error logger")
    @EditorClass.EdOptsClass(clazz = ErrorLog.class)
    private ErrorLog errorLog;

    @ConfigurableField(editor = EditorString.class, optional = true,
            label = "ParentKey", description = "The key used to store the parent object of the result node")
    @EditorString.EdOptsString(dflt = "parent")
    private String parentKey = "parent";

    private SensorThingsService service;

    @Override
    public void init(SensorThingsService service) throws ImportException {
        this.service = service;
        main.init(service);
    }

    @Override
    public InspectingIterator<Tuple> iterator() {
        return new TupleIterator(this, path);
    }

    private static final class TupleIterator implements InspectingIterator<Tuple> {

        private final TupleSourceJsonExtender tsj;
        private final InspectingIterator<Tuple> mainIterator;
        private Tuple mainTuple;
        private ObjectNode mainNode;

        private InspectingIterator<Tuple> subIterator;
        private Tuple subTuple;

        public TupleIterator(TupleSourceJsonExtender parent, String path) {
            this.tsj = parent;
            mainIterator = parent.main.iterator();

            findNext();
        }

        @Override
        public String getCurrentLocation() {
            return mainIterator.getCurrentLocation();
        }

        @Override
        public boolean hasNext() {
            return subTuple != null;
        }

        @Override
        public Tuple next() {
            if (subTuple == null) {
                return null;
            }

            Tuple finalTuple = subTuple;
            final Object source = finalTuple.getSource();
            if (source instanceof ObjectNode onSub) {
                onSub.putIfAbsent(tsj.extensionPath, mainNode);
            }
            findNext();
            return finalTuple;
        }

        private void findNext() {
            subTuple = null;
            while (subIterator == null || !subIterator.hasNext()) {
                if (!mainIterator.hasNext()) {
                    // No subIterator and mainIterator is done.
                    return;
                }
                nextMain();
            }
            subTuple = subIterator.next();
        }

        private void nextMain() {
            mainTuple = mainIterator.next();
            final Object source = mainTuple.getSource();
            if (source instanceof ObjectNode on) {
                mainNode = on;
            }
            String baseUrl = Translator.fillTemplate(tsj.baseUrlTemplate, mainTuple, Translator.StringType.PLAIN, true);
            TupleSourceJson subSource = new TupleSourceJson()
                    .setInput(new DataGeneratorFixed().setBaseUrl(baseUrl))
                    .setParentKey(tsj.parentKey)
                    .setPath(tsj.path);
            subSource.init(tsj.service);
            subIterator = subSource.iterator();
        }

    }

}
