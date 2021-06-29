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
import de.fraunhofer.iosb.ilt.configurable.Utils;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.UrlUtils;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
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

	@Override
	public Iterator<URL> iterator() {
		return new proxyIterator(this, inputUrl.iterator());
	}

	private static class proxyIterator implements Iterator<URL> {

		private final UrlGeneratorBouncer bouncer;
		private final Iterator<URL> parentIterator;
		private Iterator<String> currentIterator;
		private Pattern filter;
		private String splitter;
		private URL currentParent;

		public proxyIterator(UrlGeneratorBouncer bouncer, Iterator<URL> parentIterator) {
			this.bouncer = bouncer;
			this.parentIterator = parentIterator;
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
		public boolean hasNext() {
			return parentIterator.hasNext() || currentIterator.hasNext();
		}

		@Override
		public URL next() {
			try {
				if (currentIterator.hasNext()) {
					String next = currentIterator.next();
					final int hasHttp = next.indexOf("http");
					if (hasHttp >= 0) {
						next = next.substring(hasHttp);
					}
					return new URL(currentParent, next);
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
				try {
					currentParent = new URL(nextParentUrl.toString().trim());
					fetchedFromUrl = UrlUtils.fetchFromUrl(currentParent.toString());
					String[] split = StringUtils.split(fetchedFromUrl, splitter);
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
					LOGGER.error("Failed to handle URL: {}; {}", currentParent, exc.getMessage());
				}
			}
		}
	}

}
