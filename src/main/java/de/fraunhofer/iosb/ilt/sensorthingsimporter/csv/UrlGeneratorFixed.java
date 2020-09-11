/*
 * Copyright (C) 2020 Fraunhofer IOSB
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

import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;

/**
 *
 * @author hylke
 */
public class UrlGeneratorFixed implements UrlGenerator, AnnotatedConfigurable<Object, Object> {

	@ConfigurableField(editor = EditorString.class,
			label = "Url", description = "The URL.")
	@EditorString.EdOptsString()
	private String url;

	@Override
	public Iterator<URL> iterator() {
		try {
			URL u = new URL(url);
			return Arrays.asList(u).iterator();
		} catch (MalformedURLException ex) {
			throw new IllegalArgumentException("Not a valid URL: " + url, ex);
		}
	}

}
