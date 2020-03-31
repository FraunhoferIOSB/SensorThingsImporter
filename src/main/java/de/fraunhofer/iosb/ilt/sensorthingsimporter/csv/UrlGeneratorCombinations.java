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
import de.fraunhofer.iosb.ilt.configurable.editor.EditorClass;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorList;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author hylke
 */
public class UrlGeneratorCombinations implements UrlGenerator, AnnotatedConfigurable<Object, Object> {

	public static class ReplaceSet implements AnnotatedConfigurable<Object, Object> {

		@ConfigurableField(editor = EditorString.class,
				label = "Key", description = "The placeholder {key} to replace in the base String.")
		@EditorString.EdOptsString()
		private String replaceKey;

		@ConfigurableField(editor = EditorList.class,
				label = "Replacements", description = "The values to replace {key} with.")
		@EditorList.EdOptsList(editor = EditorString.class, minCount = 1, labelText = "Add Replace Value")
		@EditorString.EdOptsString()
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
			return replacements;
		}

		public ReplaceSet addReplacement(String replacement) {
			if (replacements == null) {
				replacements = new ArrayList<>();

			}
			replacements.add(replacement);
			return this;
		}

		private void init() {
			if (iterator == null) {
				iterator = replacements.iterator();
				if (child != null) {
					child.childInit();
				}
			}
		}

		private void childInit() {
			iterator = replacements.iterator();
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
				iterator = replacements.iterator();
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

	@ConfigurableField(editor = EditorList.class,
			label = "ReplaceSets", description = "The sets of replacements for each placeholder in the Base URL.")
	@EditorList.EdOptsList(editor = EditorClass.class, minCount = 1, labelText = "Add Replace Set")
	@EditorClass.EdOptsClass(clazz = ReplaceSet.class)
	private List<ReplaceSet> replaceSets = new ArrayList<>();

	@Override
	public Iterator<URL> iterator() {
		return new ComboIterator(baseUrl, replaceSets);
	}

	private class ComboIterator implements Iterator<URL> {

		private final String baseUrl;
		private ReplaceSet start;

		public ComboIterator(String baseUrl, List<ReplaceSet> replaceSets) {
			this.baseUrl = baseUrl;
			init(replaceSets);
		}

		private void init(List<ReplaceSet> replaceSets) {
			ReplaceSet last = null;
			for (ReplaceSet curent : replaceSets) {
				if (last == null) {
					start = curent;
				} else {
					last.setChild(curent);
				}
				last = curent;
			}
		}

		@Override
		public boolean hasNext() {
			return start.hasNext();
		}

		@Override
		public URL next() {
			start.next();
			String finalUrl = start.replace(baseUrl);
			try {
				return new URL(finalUrl);
			} catch (MalformedURLException ex) {
				throw new IllegalArgumentException("Not a valid URL: " + finalUrl, ex);
			}
		}
	}

	public String getBaseUrl() {
		return baseUrl;
	}

	public void setBaseUrl(String baseUrl) {
		this.baseUrl = baseUrl;
	}

	public void setReplaceSets(List<ReplaceSet> replaceSets) {
		this.replaceSets = replaceSets;
	}

	public void addReplaceSet(ReplaceSet set) {
		replaceSets.add(set);
	}

}
