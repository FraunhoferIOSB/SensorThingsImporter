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
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UrlUtils;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hylke
 */
public class UrlGeneratorBouncer implements UrlGenerator, AnnotatedConfigurable<Object, Object> {

	private static final Logger LOGGER = LoggerFactory.getLogger(UrlGeneratorBouncer.class.getName());

	@ConfigurableField(editor = EditorSubclass.class,
			label = "Input Url", description = "The input url(s)")
	@EditorSubclass.EdOptsSubclass(iface = UrlGenerator.class)
	private UrlGenerator inputUrl;

	@ConfigurableField(
			label = "Sort",
			description = "Sort the urls received from the input url.",
			editor = EditorBoolean.class)
	@EditorBoolean.EdOptsBool()
	private boolean sort;

	@Override
	public Iterator<URL> iterator() {
		return new proxyIterator(inputUrl.iterator());
	}

	private class proxyIterator implements Iterator<URL> {

		private final Iterator<URL> parentIterator;
		private Iterator<String> currentIterator;

		public proxyIterator(Iterator<URL> parentIterator) {
			this.parentIterator = parentIterator;
			try {
				nextParent();
			} catch (ImportException ex) {
				throw new IllegalStateException(ex);
			}
		}

		@Override
		public boolean hasNext() {
			return parentIterator.hasNext() || currentIterator.hasNext();
		}

		@Override
		public URL next() {
			try {
				if (currentIterator.hasNext()) {
					String next = currentIterator.next();
					next = next.substring(next.indexOf("http"));
					return new URL(next);
				} else {
					if (!parentIterator.hasNext()) {
						return null;
					}
					nextParent();
					return next();
				}
			} catch (MalformedURLException | ImportException ex) {
				throw new IllegalStateException(ex);
			}
		}

		private void nextParent() throws ImportException {
			if (parentIterator.hasNext()) {
				URL nextParentUrl = parentIterator.next();
				String fetchedFromUrl;
				final String inUrl = nextParentUrl.toString().trim();
				try {
					fetchedFromUrl = UrlUtils.fetchFromUrl(inUrl);
					String[] split = StringUtils.split(fetchedFromUrl, "\n\r ");
					List<String> asList;
					if (fetchedFromUrl.isEmpty() || split.length == 0) {
						asList = Collections.emptyList();
					} else {
						asList = Arrays.asList(split);
					}
					if (sort) {
						asList.sort(null);
					}
					currentIterator = asList.iterator();
				} catch (IOException exc) {
					LOGGER.error("Failed to handle URL: {}; {}", inUrl, exc.getMessage());
				}
			}
		}

	}

}
