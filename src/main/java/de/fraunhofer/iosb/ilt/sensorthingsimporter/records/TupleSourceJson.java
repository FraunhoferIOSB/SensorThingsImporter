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
import de.fraunhofer.iosb.ilt.frostclient.json.SimpleJsonMapper;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.datagen.DataGenerator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.records.Tuples.JsonTuple;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.CollectionsHelper;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ErrorLog;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.InspectingIterator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UrlUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * Turns a JSON Document into tuples. Can work on nested objects and arrays.
 *
 * []/timeseries/[]
 */
public class TupleSourceJson implements TupleSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(TupleSourceJson.class.getName());

    @ConfigurableField(editor = EditorSubclass.class,
            label = "Input", description = "The input")
    @EditorSubclass.EdOptsSubclass(iface = DataGenerator.class)
    private DataGenerator input;

    @ConfigurableField(editor = EditorString.class,
            label = "Path", description = "The path in the JSON document, using [] for arrays that should be iterated over.")
    @EditorString.EdOptsString(dflt = "[]/timeseries/[]")
    private String path = "[]/timeseries/[]";

    @ConfigurableField(editor = EditorClass.class, optional = true,
            label = "Error Logger", description = "Configuration of the error logger")
    @EditorClass.EdOptsClass(clazz = ErrorLog.class)
    private ErrorLog errorLog;

    @ConfigurableField(editor = EditorString.class, optional = true,
            label = "ParentKey", description = "The key used to store the parent object of the result node")
    @EditorString.EdOptsString(dflt = "parent")
    private String parentKey = "parent";

    @Override
    public void init(SensorThingsService service) throws ImportException {
        input.init(service);
    }

    @Override
    public InspectingIterator<Tuple> iterator() {
        return new TupleIterator(this, path);
    }

    public DataGenerator getInput() {
        return input;
    }

    public TupleSourceJson setInput(DataGenerator input) {
        this.input = input;
        return this;
    }

    public String getPath() {
        return path;
    }

    public TupleSourceJson setPath(String path) {
        this.path = path;
        return this;
    }

    public String getParentKey() {
        return parentKey;
    }

    public TupleSourceJson setParentKey(String parentKey) {
        this.parentKey = parentKey;
        return this;
    }

    private final class TupleIterator implements InspectingIterator<Tuple> {

        private final TupleSourceJson tsj;
        private final InspectingIterator<UrlUtils.HttpResponse> dataIterator;

        private final List<String> pathToArr = new ArrayList<>();
        private final List<String> pathInArr = new ArrayList<>();

        private JsonNodeIterator arrayIter;
        private int currentLine;
        private ObjectNode nextItem;
        private ObjectNode parentNode;

        public TupleIterator(TupleSourceJson parent, String path) {
            this.tsj = parent;
            this.dataIterator = parent.getInput().items(errorLog).iterator();
            String[] pathItems = StringUtils.split(path, '/');
            boolean arrayFound = false;
            for (var item : pathItems) {
                if (arrayFound) {
                    pathInArr.add(item);
                } else {
                    if ("[]".equals(item)) {
                        arrayFound = true;
                    } else {
                        pathToArr.add(item);
                    }
                }
            }
            nextItem = findNext();
        }

        @Override
        public String getCurrentLocation() {
            return "row " + currentLine + " of " + dataIterator.getCurrentLocation();
        }

        @Override
        public boolean hasNext() {
            return nextItem != null;
        }

        @Override
        public JsonTuple next() {
            if (nextItem == null) {
                return null;
            }
            ObjectNode curItem = nextItem.deepCopy();
            curItem.putIfAbsent(getParentKey(), parentNode);
            nextItem = findNext();
            return new JsonTuple(curItem);
        }

        private ObjectNode findNext() {
            while (arrayIter == null || !arrayIter.hasNext()) {
                if (dataIterator.hasNext()) {
                    UrlUtils.HttpResponse dataResponse = dataIterator.next();
                    if (dataResponse == null) {
                        LOGGER.error("No valid input url or file.");
                        return null;
                    }
                    try {
                        JsonNode tree = SimpleJsonMapper.getSimpleObjectMapper().readTree(dataResponse.getDataReader());
                        Object targetNode = CollectionsHelper.getFrom(tree, pathToArr);
                        if (targetNode instanceof ArrayNode an) {
                            arrayIter = new JsonNodeIterator(tsj, an, parentNode, pathInArr);
                        } else {
                            LOGGER.error("Not an array node: {}", targetNode);
                        }
                    } catch (IOException exc) {
                        LOGGER.error("Failed to handle URL: {}; {}", dataIterator.getCurrentLocation(), exc.getMessage());
                    }
                } else {
                    // no arrayIter and dataIter is done, thus We're done.
                    return null;
                }
            }
            return arrayIter.next();
        }

    }

    private static final class JsonNodeIterator implements Iterator<JsonNode> {

        private final TupleSourceJson tsj;

        private final JsonNode parentNode;
        private final Iterator<JsonNode> mainIter;
        int itemNr = 0;

        private final boolean hasSub;
        private final List<String> pathToSub = new ArrayList<>();
        private final List<String> pathInSub = new ArrayList<>();
        private JsonNodeIterator subIter;

        private ObjectNode nextNode;

        public JsonNodeIterator(TupleSourceJson tsj, ArrayNode arrayNode, JsonNode parentNode, List<String> path) {
            this.tsj = tsj;
            this.parentNode = parentNode;
            mainIter = arrayNode.iterator();
            boolean foundSub = false;
            for (var item : path) {
                if (foundSub) {
                    pathInSub.add(item);
                } else {
                    if ("[]".equals(item)) {
                        foundSub = true;
                    } else {
                        pathToSub.add(item);
                    }
                }
            }
            hasSub = foundSub;
            nextNode = findNext();
        }

        @Override
        public boolean hasNext() {
            return nextNode != null;
        }

        @Override
        public ObjectNode next() {
            if (nextNode == null) {
                return null;
            }
            ObjectNode tempNext = nextNode.deepCopy();
            tempNext.putIfAbsent(tsj.getParentKey(), parentNode);
            nextNode = findNext();
            return tempNext;
        }

        private ObjectNode findNext() {
            if (hasSub) {
                while (subIter == null || !subIter.hasNext()) {
                    if (mainIter.hasNext()) {
                        itemNr++;
                        JsonNode nextMain = mainIter.next();
                        if (nextMain.isObject()) {
                            ObjectNode nextMainObj = nextMain.asObject().deepCopy();
                            nextMainObj.putIfAbsent(tsj.getParentKey(), parentNode);
                        }
                        Object subArray = CollectionsHelper.getFrom(nextMain, pathToSub);
                        if (subArray instanceof ArrayNode an) {
                            subIter = new JsonNodeIterator(tsj, an, nextMain, pathInSub);
                        } else {
                            tsj.errorLog.addError("NoArray", Objects.toString(pathToSub), itemNr);
                        }
                    } else {
                        // no subIter and mainIter is done, thus We're done.
                        return null;
                    }
                }
                return subIter.next();
            } else {
                while (mainIter.hasNext()) {
                    itemNr++;
                    JsonNode found = mainIter.next();
                    if (found.isObject()) {
                        return found.asObject();
                    } else {
                        LOGGER.error("Found non-object node {}", found);
                    }
                }
                // mainIter is done, thus We're done.
                return null;
            }
        }

    }
}
