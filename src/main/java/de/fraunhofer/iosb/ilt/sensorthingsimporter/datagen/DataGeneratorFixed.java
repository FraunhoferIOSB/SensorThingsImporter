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
import de.fraunhofer.iosb.ilt.frostclient.SensorThingsService;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ErrorLog;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.InspectingIterable;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.InspectingIterator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UrlUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UrlUtils.HttpResponse;
import java.io.IOException;

public class DataGeneratorFixed implements DataGenerator {

    @ConfigurableField(editor = EditorString.class,
            label = "BaseUrl", description = "The base URL with replace placeholders.")
    @EditorString.EdOptsString()
    private String baseUrl;

    @ConfigurableField(editor = EditorBoolean.class, optional = true,
            label = "IsPost", description = "Use a POST with the bodyTemplate instead of a GET.")
    @EditorBoolean.EdOptsBool()
    private boolean post;

    @ConfigurableField(editor = EditorString.class, optional = true,
            label = "BodyTemplate", description = "The body for the post.")
    @EditorString.EdOptsString(lines = 5)
    private String bodyTemplate;

    @Override
    public void init(SensorThingsService service) throws ImportException {
        // Nothing to initialise.
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public DataGeneratorFixed setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
        return this;
    }

    public String getBodyTemplate() {
        return bodyTemplate;
    }

    public DataGeneratorFixed setBodyTemplate(String bodyTemplate) {
        this.bodyTemplate = bodyTemplate;
        return this;
    }

    public boolean isPost() {
        return post;
    }

    public DataGeneratorFixed setPost(boolean post) {
        this.post = post;
        return this;
    }

    @Override
    public InspectingIterable<HttpResponse> items(ErrorLog errorLog) {
        return new ComboIterable(this);
    }

    private static class ComboIterable implements InspectingIterable<HttpResponse> {

        private final DataGeneratorFixed parent;

        public ComboIterable(DataGeneratorFixed parent) {
            this.parent = parent;
        }

        @Override
        public InspectingIterator<HttpResponse> iterator() {
            return new ComboIterator(parent);
        }

    }

    private static class ComboIterator implements InspectingIterator<HttpResponse> {

        private final DataGeneratorFixed parent;
        private boolean done;

        public ComboIterator(DataGeneratorFixed parent) {
            this.parent = parent;
        }

        @Override
        public String getCurrentLocation() {
            return parent.baseUrl;
        }

        @Override
        public boolean hasNext() {
            return !done;
        }

        @Override
        public HttpResponse next() {
            done = true;
            String finalUrl = parent.getBaseUrl();
            String finalBody = parent.getBodyTemplate();
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
