/*
 * Copyright (C) 2024 Fraunhofer IOSB
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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.csv;

import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorList;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import java.util.Collections;
import java.util.List;

public class StringListGenStatic implements StringListGenerator {

	@ConfigurableField(editor = EditorList.class,
			label = "Replacements", description = "The values to replace {key} with.")
	@EditorList.EdOptsList(editor = EditorString.class, minCount = 1, labelText = "Add Replace Value")
	@EditorString.EdOptsString()
	private List<String> strings;

	@Override
	public List<String> get() {
		if (strings == null) {
			return Collections.emptyList();
		}
		return strings;
	}

}
