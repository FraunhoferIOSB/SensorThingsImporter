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

import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorClass;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorList;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.csv.StringListGenerator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ErrorLog;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.InspectingIterable;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.InspectingIterator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UrlUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UrlUtils.HttpResponse;
import de.fraunhofer.iosb.ilt.swe.common.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class DataGeneratorTemplate implements DataGenerator {

    public static class ReplaceSet implements AnnotatedConfigurable<Object, Object> {

        @ConfigurableField(editor = EditorString.class,
                label = "Key", description = "The placeholder {key} to replace in the base String.")
        @EditorString.EdOptsString()
        private String replaceKey;

        @ConfigurableField(editor = EditorSubclass.class,
                label = "Generator", description = "Generator for the values to replace {key} with.")
        @EditorSubclass.EdOptsSubclass(iface = StringListGenerator.class, merge = true, shortenClassNames = true)
        private StringListGenerator replacementGen;

        private List<String> replacements;

        private Iterator<String> iterator;

        private String current;

        private ReplaceSet child;

        public ReplaceSet() {
        }

        public void setChild(ReplaceSet child) {
            this.child = child;
        }

        public String getReplaceKey() {
            return replaceKey;
        }

        public ReplaceSet setReplaceKey(String replaceKey) {
            this.replaceKey = replaceKey;
            return this;
        }

        public List<String> getReplacements() {
            if (Utils.isNullOrEmpty(replacements)) {
                if (replacementGen != null) {
                    replacements = replacementGen.get();
                } else {
                    replacements = new ArrayList<>();
                }
            }
            return replacements;
        }

        public ReplaceSet addReplacement(String replacement) {
            getReplacements().add(replacement);
            return this;
        }

        private void init() {
            if (iterator == null) {
                iterator = getReplacements().iterator();
                if (child != null) {
                    child.childInit();
                }
            }
        }

        private void childInit() {
            iterator = getReplacements().iterator();
            current = iterator.next();
            if (child != null) {
                child.childInit();
            }
        }

        public boolean hasNext() {
            init();
            return iterator.hasNext() || (child != null && child.hasNext());
        }

        public void next() {
            init();
            if (!iterator.hasNext()) {
                iterator = getReplacements().iterator();
                child.next();
            }
            current = iterator.next();
        }

        public String replace(String input) {
            String value = StringUtils.replace(input, replaceKey, current);
            if (child == null) {
                return value;
            }
            return child.replace(value);
        }

        public void reset() {
            iterator = null;
            if (child != null) {
                child.reset();
            }
        }
    }

    @ConfigurableField(editor = EditorString.class,
            label = "BaseUrl", description = "The base URL with replace placeholders.")
    @EditorString.EdOptsString()
    private String baseUrl;

    @ConfigurableField(editor = EditorBoolean.class, optional = true,
            label = "IsPost", description = "Use a POST with the bodyTemplate instead of a GET.")
    @EditorBoolean.EdOptsBool()
    private boolean post;

    @ConfigurableField(editor = EditorString.class, optional = true,
            label = "BodyTemplate", description = "The template to use for the post body.")
    @EditorString.EdOptsString(lines = 5)
    private String bodyTemplate;

    @ConfigurableField(editor = EditorList.class,
            label = "ReplaceSets", description = "The sets of replacements for each placeholder in the Base URL or the POST body.")
    @EditorList.EdOptsList(editor = EditorClass.class, minCount = 0, labelText = "Add Replace Set")
    @EditorClass.EdOptsClass(clazz = ReplaceSet.class)
    private List<ReplaceSet> replaceSets = new ArrayList<>();

    public String getBaseUrl() {
        return baseUrl;
    }

    public DataGeneratorTemplate setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
        return this;
    }

    public String getBodyTemplate() {
        return bodyTemplate;
    }

    public DataGeneratorTemplate setBodyTemplate(String bodyTemplate) {
        this.bodyTemplate = bodyTemplate;
        return this;
    }

    public boolean isPost() {
        return post;
    }

    public DataGeneratorTemplate setPost(boolean post) {
        this.post = post;
        return this;
    }

    public List<ReplaceSet> getReplaceSets() {
        return replaceSets;
    }

    public DataGeneratorTemplate setReplaceSets(List<ReplaceSet> replaceSets) {
        this.replaceSets = replaceSets;
        return this;
    }

    @Override
    public InspectingIterable<HttpResponse> items(ErrorLog errorLog) {
        return new ComboIterable(this);
    }

    private static class ComboIterable implements InspectingIterable<HttpResponse> {

        private final DataGeneratorTemplate parent;

        public ComboIterable(DataGeneratorTemplate parent) {
            this.parent = parent;
        }

        @Override
        public InspectingIterator<HttpResponse> iterator() {
            return new ComboIterator(parent);
        }

    }

    private static class ComboIterator implements InspectingIterator<HttpResponse> {

        private final DataGeneratorTemplate parent;
        private ReplaceSet start;

        public ComboIterator(DataGeneratorTemplate parent) {
            this.parent = parent;
            init();
        }

        private void init() {
            ReplaceSet last = null;
            for (ReplaceSet curent : parent.getReplaceSets()) {
                if (last == null) {
                    start = curent;
                } else {
                    last.setChild(curent);
                }
                last = curent;
            }
        }

        @Override
        public String getCurrentLocation() {
            return parent.baseUrl;
        }

        @Override
        public boolean hasNext() {
            return start != null && start.hasNext();
        }

        @Override
        public HttpResponse next() {
            start.next();
            String finalUrl = start.replace(parent.getBaseUrl());
            String finalBody = start.replace(parent.getBodyTemplate());
            try {
                if (parent.isPost()) {
                    return UrlUtils.postToUrl(finalUrl, finalBody, null, null);
                }
                return UrlUtils.fetchFromUrl(finalUrl);
            } catch (IOException ex) {
                throw new IllegalArgumentException("Failed to request URL: " + finalUrl, ex);
            }
        }
    }

}
