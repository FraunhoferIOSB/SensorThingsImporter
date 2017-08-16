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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.importers;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorClass;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.TimeObject;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.LoggerFactory;

/**
 * Converts Json to Observations.
 *
 * @author scf
 */
public class JsonConverter implements DocumentParser {

	/**
	 * The logger for this class.
	 */
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(JsonConverter.class);

	private EditorMap<Map<String, Object>> editor;
	private EditorClass<SensorThingsService, Object, ParserTime> editorTimeParser;
	private EditorSubclass<SensorThingsService, Object, Parser> editorResultParser;
	private EditorString editorPathList;
	private EditorString editorPathPhenTime;
	private EditorString editorPathResult;

	private String[] listPathParts;
	private String[] phenTimePathParts;
	private String[] resultPathParts;
	private Parser resultParser;
	private ParserTime timeParser;

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx) {
		getConfigEditor(context, edtCtx).setConfig(config);
		String listPath = editorPathList.getValue();
		listPathParts = listPath.split("/");
		String phenTimePath = editorPathPhenTime.getValue();
		phenTimePathParts = phenTimePath.split("/");
		String resultPath = editorPathResult.getValue();
		resultPathParts = resultPath.split("/");
		resultParser = editorResultParser.getValue();
		timeParser = editorTimeParser.getValue();
	}

	@Override
	public ConfigEditor<?> getConfigEditor(SensorThingsService context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorMap<>();

			editorTimeParser = new EditorClass<>(context, edtCtx, ParserTime.class, "Time Parser", "The parser to use for parsing times.");
			editor.addOption("timeParser", editorTimeParser, false);

			editorResultParser = new EditorSubclass<>(context, edtCtx, Parser.class, "Result Parser", "The parser to use for parsing results.");
			editor.addOption("resultParser", editorResultParser, false);

			editorPathList = new EditorString("rows", 1, "List Path", "The path in the JSON document that points the the array holding the observations.");
			editor.addOption("pathList", editorPathList, false);

			editorPathPhenTime = new EditorString("DATAORA", 1, "PhenomenonTime Path", "The path inside each element in the list, that holds the phenomenonTime");
			editor.addOption("pathPhenTime", editorPathPhenTime, false);

			editorPathResult = new EditorString("VALORE", 1, "Result Path", "The path inside each element in the list, that holds the result");
			editor.addOption("pathResult", editorPathResult, false);

		}
		return editor;

	}

	@Override
	public List<Observation> process(Datastream ds, String input) throws ImportException {
		try {
			ObjectMapper mapper = new ObjectMapper();
			mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
			JsonNode json = mapper.readTree(input);

			JsonNode listJson = walk(json, listPathParts);
			if (!listJson.isArray()) {
				throw new ImportException("List path did not lead to an array.");
			}

			List<Observation> observationList = new ArrayList<>();
			for (JsonNode element : listJson) {
				ZonedDateTime phenTime = timeParser.parse(walk(element, phenTimePathParts).asText());
				Object result = resultParser.parse(walk(element, resultPathParts));
				Observation obs = new Observation(result, ds);
				obs.setPhenomenonTime(new TimeObject(phenTime));
				observationList.add(obs);
			}

			return observationList;
		} catch (IOException ex) {
			LOGGER.error("Failed to parse.", ex);
			throw new ImportException(ex);
		}
	}

	@Override
	public List<Observation> process(MultiDatastream mds, String... inputs) throws ImportException {
		Map<ZonedDateTime, Observation> observationsMap = new HashMap<>();
		ObjectMapper mapper = new ObjectMapper();
		int resultIndex = 0;
		try {
			for (String input : inputs) {
				JsonNode json = mapper.readTree(input);

				JsonNode listJson = walk(json, listPathParts);
				if (!listJson.isArray()) {
					throw new ImportException("List path did not lead to an array.");
				}

				Map<ZonedDateTime, Observation> updatedMap = new HashMap<>();
				for (JsonNode element : listJson) {
					ZonedDateTime phenTime = timeParser.parse(walk(element, phenTimePathParts).asText());
					Object result = resultParser.parse(walk(element, resultPathParts).asText());
					if (resultIndex == 0) {
						Object[] resultArr = new Object[inputs.length];
						resultArr[resultIndex] = result;
						Observation obs = new Observation(resultArr, mds);
						obs.setPhenomenonTime(new TimeObject(phenTime));
						updatedMap.put(phenTime, obs);
					} else {
						Observation obs = observationsMap.get(phenTime);
						if (obs != null) {
							((Object[]) obs.getResult())[resultIndex] = result;
							updatedMap.put(phenTime, obs);
						}
					}
				}
				resultIndex++;
				observationsMap.clear();
				observationsMap.putAll(updatedMap);
			}
			return new ArrayList<>(observationsMap.values());
		} catch (IOException ex) {
			LOGGER.error("Failed to parse.", ex);
			throw new ImportException(ex);
		}
	}

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
}
