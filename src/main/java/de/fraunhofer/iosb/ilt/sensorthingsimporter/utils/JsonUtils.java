/*
 * Copyright (C) 2017 Fraunhofer IOSB
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
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class JsonUtils {

	public static TypeReference TYPE_MAP_STRING_OBJECT = new TypeReference<Map<String, Object>>() {
		// Empty on purpose.
	};
	public static Charset UTF_8 = Charset.forName("UTF-8");

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(JsonUtils.class);

	private static ObjectMapper mapper;

	public static JsonNode walk(final JsonNode node, final String[] pathParts) {
		JsonNode curNode = node;
		for (String pathPart : pathParts) {
			if (pathPart.isEmpty()) {
				continue;
			}
			if (curNode.isArray()) {
				try {
					int arrIndex = Integer.parseInt(pathPart);
					curNode = curNode.path(arrIndex);
				} catch (NumberFormatException exc) {
					LOGGER.warn("Array must be traversed with index. Could not parse {} to integer.", pathPart);
				}
			} else {
				curNode = curNode.path(pathPart);
			}
		}
		return curNode;
	}

	public static JsonNode walk(final JsonNode node, final String path) {
		final String[] pathParts = path.split("/");
		return walk(node, pathParts);
	}

	public static int toInt(Object o) {
		if (o == null) {
			throw new IllegalArgumentException("Parameter must be non-null");
		}
		return toInt(o, 0);
	}

	public static int toInt(Object o, int deflt) {
		if (o == null) {
			return deflt;
		}
		if (o instanceof Integer) {
			Integer integer = (Integer) o;
			return integer;
		}
		if (o instanceof Number) {
			Number number = (Number) o;
			return number.intValue();
		}
		return Integer.parseInt(o.toString());
	}

	public static ObjectMapper getMapper() {
		if (mapper == null) {
			mapper = new ObjectMapper();
			mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
		}
		return mapper;
	}

	public static Map<String, Object> jsonToMap(String json) {
		try {
			return getMapper().readValue(json, TYPE_MAP_STRING_OBJECT);
		} catch (IOException ex) {
			LOGGER.warn("Failed to parse json to Map: {}", ex.getMessage());
			LOGGER.debug("Exception: ", ex);
			return new HashMap<>();
		}
	}
}
