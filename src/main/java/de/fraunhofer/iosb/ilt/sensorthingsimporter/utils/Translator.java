/*
 * Copyright (C) 2018 Fraunhofer IOSB
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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.AbstractConfigurable;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.ConfigurationException;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.sta.Utils;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class Translator extends AbstractConfigurable<Void, Void> {

	public static enum StringType {
		PLAIN,
		URL,
		JSON
	}

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(Translator.class);
	private static final Pattern PLACE_HOLDER_PATTERN = Pattern.compile("\\{([0-9a-zA-Z_]+)(\\|([^}]*))?\\}");

	private static final TypeReference<Map<String, String>> TYPE_REF_MAP_STRING_STRING = new TypeReference<Map<String, String>>() {
		// Empty by design.
	};
	private final Map<String, String> replaces = new HashMap<>();

	@ConfigurableField(
			label = "Translations", description = "A map that translates input values to url values.",
			editor = EditorString.class)
	@EditorString.EdOptsString(lines = 10, dflt = "{\"from string\":\"to string\"}")
	private String mappings;

	@Override
	public void configure(JsonElement config, Void context, Void edtCtx, ConfigEditor<?> configEditor) throws ConfigurationException {
		super.configure(config, context, edtCtx, configEditor);
		setMappings(mappings);
	}

	public void setMappings(String json) {
		try {
			ObjectMapper mapper = new ObjectMapper();
			replaces.putAll(mapper.readValue(json, TYPE_REF_MAP_STRING_STRING));
		} catch (IOException ex) {
			throw new RuntimeException("Failed to parse mapping.", ex);
		}
	}

	public String translate(String input) {
		String result = replaces.get(input);
		if (result == null) {
			return input;
		}
		return result;
	}

	public void put(String input, String output) {
		replaces.put(input, output);
	}

	public String replaceIn(String source, boolean urlEncode) {
		Pattern pattern = Pattern.compile(Pattern.quote("{") + "([^}]+)}");
		Matcher matcher = pattern.matcher(source);
		StringBuilder result = new StringBuilder();
		int lastEnd = 0;
		while (matcher.find()) {
			int end = matcher.end();
			int start = matcher.start();
			result.append(source.substring(lastEnd, start));
			String key = matcher.group(1);
			String value = replaces.get(key);
			if (value == null) {
				LOGGER.error("No replacement for {}", key);
			}
			if (urlEncode) {
				try {
					result.append(URLEncoder.encode(value, "UTF-8"));
				} catch (UnsupportedEncodingException ex) {
					LOGGER.error("UTF-8 not supported??", ex);
				}
			} else {
				result.append(value);
			}
			lastEnd = end;
		}
		if (lastEnd < source.length()) {
			result.append(source.substring(lastEnd, source.length()));
		}
		return result.toString();
	}

	public static String fillTemplate(String template, CSVRecord record, StringType targetType, boolean removeNewlines) {
		if (removeNewlines) {
			return fillTemplate(StringUtils.remove(template, "\n"), record, targetType);
		}
		return fillTemplate(template, record, targetType);
	}

	public static String fillTemplate(String template, CSVRecord record) {
		return fillTemplate(template, record, StringType.PLAIN);
	}

	public static String fillTemplate(String template, CSVRecord record, StringType targetType) {
		if (record.isMapped(template)) {
			return record.get(template);
		}
		Matcher matcher = PLACE_HOLDER_PATTERN.matcher(template);
		matcher.reset();
		StringBuilder filter = new StringBuilder();
		int pos = 0;
		while (matcher.find()) {
			int start = matcher.start();
			filter.append(template.substring(pos, start));
			String value;
			try {
				int colNr = Integer.parseInt(matcher.group(1));
				value = record.get(colNr);
			} catch (NumberFormatException ex) {
				String colName = matcher.group(1);
				value = record.get(colName);
			}
			if (Utils.isNullOrEmpty(value) && matcher.group(3) != null) {
				value = matcher.group(3);
			}
			switch (targetType) {
				case JSON:
					value = StringUtils.replace(value, "\\", "\\\\");
					value = StringUtils.replace(value, "\n", "\\n");
					break;

				case URL:
					value = Utils.escapeForStringConstant(value);
					break;

				case PLAIN:
				default:
				// Do nothing.
			}
			filter.append(value);
			pos = matcher.end();
		}
		filter.append(template.substring(pos));
		return filter.toString();
	}

}
