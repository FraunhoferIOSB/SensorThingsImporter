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
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.frostclient.SensorThingsService;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.records.Tuple;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.records.TupleSource;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator;
import java.util.ArrayList;
import java.util.List;

public class StringListGenTuples implements StringListGenerator {

    @ConfigurableField(editor = EditorSubclass.class,
            label = "Input", description = "The tuples to generate strings from.")
    @EditorSubclass.EdOptsSubclass(iface = TupleSource.class)
    private TupleSource input;

    @ConfigurableField(editor = EditorString.class,
            label = "Template", description = "The template to apply each Tuple to, to generate the string.")
    @EditorString.EdOptsString(dflt = "{field}")
    private String template;

    @Override
    public void init(SensorThingsService service) throws ImportException {
        if (input != null) {
            input.init(service);
        }
    }

    @Override
    public List<String> get() {
        List<String> result = new ArrayList<>();
        for (Tuple tuple : input) {
            String item = Translator.fillTemplate(template, tuple, Translator.StringType.PLAIN, true);
            result.add(item);
        }
        return result;
    }

}
