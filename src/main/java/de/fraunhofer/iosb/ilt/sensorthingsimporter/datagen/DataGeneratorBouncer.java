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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.datagen;

import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ErrorLog;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.InspectingIterable;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.InspectingIterator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UrlUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UrlUtils.HttpResponse;
import de.fraunhofer.iosb.ilt.sta.Utils;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGeneratorBouncer implements DataGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataGeneratorBouncer.class.getName());

    @ConfigurableField(editor = EditorSubclass.class,
            label = "Input Url", description = "The input url(s)")
    @EditorSubclass.EdOptsSubclass(iface = DataGenerator.class)
    private DataGenerator inputData;

    @ConfigurableField(
            label = "Sort",
            description = "Sort the urls received from the input url.",
            editor = EditorBoolean.class)
    @EditorBoolean.EdOptsBool()
    private boolean sort;

    @ConfigurableField(editor = EditorString.class,
            label = "Filter", description = "The regular expression filter to use.")
    @EditorString.EdOptsString(dflt = ".*")
    private String filterRegex;

    @ConfigurableField(editor = EditorBoolean.class,
            label = "Filter Removes", description = "If true, the filter removes matching item from the set. If false, the filter allows matching items.")
    @EditorBoolean.EdOptsBool()
    private boolean filterRemoves;

    @ConfigurableField(editor = EditorString.class, optional = true,
            label = "Splitter", description = "The characters to use to split the input into urls.")
    @EditorString.EdOptsString(dflt = "\\n\\r ")
    private String splitter;

    public DataGenerator getInputData() {
        return inputData;
    }

    public DataGeneratorBouncer setInputData(DataGenerator inputData) {
        this.inputData = inputData;
        return this;
    }

    public boolean getSort() {
        return sort;
    }

    public DataGeneratorBouncer setSort(boolean sort) {
        this.sort = sort;
        return this;
    }

    public String getFilterRegex() {
        return filterRegex;
    }

    public DataGeneratorBouncer setFilterRegex(String filterRegex) {
        this.filterRegex = filterRegex;
        return this;
    }

    public boolean getFilterRemoves() {
        return filterRemoves;
    }

    public DataGeneratorBouncer setFilterRemoves(boolean filterRemoves) {
        this.filterRemoves = filterRemoves;
        return this;
    }

    public String getSplitter() {
        return splitter;
    }

    public DataGeneratorBouncer setSplitter(String splitter) {
        this.splitter = splitter;
        return this;
    }

    @Override
    public InspectingIterable<HttpResponse> items(ErrorLog errorLog) {
        return new ProxyIterable(this, errorLog);
    }

    private static class ProxyIterable implements InspectingIterable<HttpResponse> {

        private final DataGeneratorBouncer bouncer;
        private final ErrorLog errorLog;

        public ProxyIterable(DataGeneratorBouncer bouncer, ErrorLog errorLog) {
            this.bouncer = bouncer;
            this.errorLog = errorLog;
        }

        @Override
        public InspectingIterator<HttpResponse> iterator() {
            return new ProxyIterator(bouncer, errorLog);
        }
    }

    private static class ProxyIterator implements InspectingIterator<HttpResponse> {

        private final DataGeneratorBouncer bouncer;
        private final Iterator<HttpResponse> parentIterator;
        private Iterator<String> currentIterator;
        private Pattern filter;
        private String splitter;
        private URL currentParentUrl;
        private URL currentUrl;
        private ErrorLog errorLog;

        public ProxyIterator(DataGeneratorBouncer bouncer, ErrorLog errorLog) {
            this.bouncer = bouncer;
            this.parentIterator = bouncer.inputData.items(errorLog).iterator();
            this.errorLog = errorLog;
            this.splitter = StringUtils.replaceEach(
                    bouncer.splitter,
                    new String[]{"\\n", "\\r", "\\t"},
                    new String[]{"\n", "\r", "\t"});
            if (!Utils.isNullOrEmpty(bouncer.filterRegex)) {
                filter = Pattern.compile(bouncer.filterRegex);
            }
            try {
                nextParent();
            } catch (ImportException ex) {
                throw new IllegalStateException(ex);
            }
        }

        @Override
        public String getCurrentLocation() {
            return Objects.toString(currentUrl);
        }

        @Override
        public boolean hasNext() {
            return parentIterator.hasNext() || currentIterator.hasNext();
        }

        @Override
        public HttpResponse next() {
            try {
                if (currentIterator.hasNext()) {
                    String next = currentIterator.next();
                    final int hasHttp = next.indexOf("http");
                    if (hasHttp >= 0) {
                        next = next.substring(hasHttp);
                    }
                    currentUrl = new URL(currentParentUrl, next);
                    LOGGER.debug("Next URL: {}", currentUrl);
                    return UrlUtils.fetchFromUrl(currentUrl.toString());
                } else {
                    if (!parentIterator.hasNext()) {
                        return null;
                    }
                    nextParent();
                    return next();
                }
            } catch (ImportException | IOException ex) {
                throw new IllegalStateException(ex);
            }
        }

        private void nextParent() throws ImportException {
            if (parentIterator.hasNext()) {
                HttpResponse nextParentData = parentIterator.next();
                try {
                    currentParentUrl = new URL(nextParentData.getUrl().trim());
                    String[] split = StringUtils.split(nextParentData.getDataString(), splitter);
                    List<String> inList = new ArrayList<>(split.length);
                    for (String item : split) {
                        item = item.trim();
                        item = StringUtils.replace(item, " ", "%20");
                        if (!item.isEmpty()) {
                            inList.add(item.trim());
                        }
                    }
                    List<String> outList;
                    if (filter == null) {
                        outList = inList;
                    } else {
                        outList = new ArrayList<>();
                        for (String item : inList) {
                            final boolean matches = filter.matcher(item).matches();
                            if (matches != bouncer.filterRemoves) {
                                outList.add(item);
                            }
                        }
                    }
                    if (bouncer.sort) {
                        outList.sort(null);
                    }
                    currentIterator = outList.iterator();
                } catch (IOException exc) {
                    LOGGER.error("Failed to handle URL: {}; {}", currentParentUrl, exc.getMessage());
                    errorLog.addError("Failed to download", Objects.toString(currentParentUrl), 0);
                }
            }
        }
    }

}
