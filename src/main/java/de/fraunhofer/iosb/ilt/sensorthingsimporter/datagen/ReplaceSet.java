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
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.frostclient.SensorThingsService;
import de.fraunhofer.iosb.ilt.frostclient.utils.StringHelper;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.Strings;

/**
 * Replaces a set of placeholders in a URL.
 */
public class ReplaceSet implements AnnotatedConfigurable<Object, Object> {

    @ConfigurableField(editor = EditorString.class, label = "Key", description = "The placeholder {key} to replace in the base String.")
    @EditorString.EdOptsString
    private String replaceKey;

    @ConfigurableField(editor = EditorSubclass.class, label = "Generator", description = "Generator for the values to replace {key} with.")
    @EditorSubclass.EdOptsSubclass(iface = StringListGenerator.class, merge = true, shortenClassNames = true)
    private StringListGenerator replacementGen;

    private List<String> replacements;
    private Iterator<String> iterator;
    private String current;
    private ReplaceSet child;

    public void init(SensorThingsService service) throws ImportException {
        replacementGen.init(service);
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
        if (StringHelper.isNullOrEmpty(replacements)) {
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
        String value = Strings.CS.replace(input, replaceKey, current);
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
