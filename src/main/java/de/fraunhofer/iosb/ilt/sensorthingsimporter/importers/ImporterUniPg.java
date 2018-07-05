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

import com.google.common.collect.AbstractIterator;
import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorClass;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.DsMapperFilter;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Importer;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.Utils;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.TimeObject;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

/**
 *
 * @author scf
 */
public class ImporterUniPg implements Importer {

	public static final String TAG_IMPORT_FILE_ID = "importFileId";
	public static final String TAG_IMPORT_FILE_BASE = "importFileBase";
	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ImporterUniPg.class);

	private EditorMap<Map<String, Object>> editor;
	private EditorSubclass<SensorThingsService, Object, DocumentParser> editorDocumentParser;
	private EditorString editorDataPath;
	private EditorString editorFileIdRegex;
	private EditorInt editorDuration;
	private EditorClass<Object, Object, DsMapperFilter> editorCheckDataStream;
	private EditorBoolean editorFastCheck;
	private EditorBoolean editorSkipLast;
	private EditorInt editorSleep;

	private SensorThingsService service;
	private DocumentParser docParser;
	private boolean verbose;
	private DsMapperFilter checkDataStream;
	private boolean fastcheck;
	private Integer fastCheckLastId;
	private boolean skipLast;
	private long sleepTime = 0;
	private int phenDurationDefault = 30;

	@Override
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	@Override
	public void setNoAct(boolean noAct) {
		// Nothing to set.
	}

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx) {
		service = context;
		getConfigEditor(context, edtCtx).setConfig(config);
		docParser = editorDocumentParser.getValue();
		checkDataStream = editorCheckDataStream.getValue();
		fastcheck = editorFastCheck.getValue();
		skipLast = editorSkipLast.getValue();
		sleepTime = 1000 * editorSleep.getValue();
	}

	@Override
	public ConfigEditor<?> getConfigEditor(SensorThingsService context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorMap<>();

			editorDocumentParser = new EditorSubclass<>(context, edtCtx, DocumentParser.class, "DocumentParser", "The parser that transforms a document into Observations.");
			editor.addOption("documentParser", editorDocumentParser, false);

			editorFileIdRegex = new EditorString("", 1, "FileIdRegex", "A regular expression extracting an ID from the filename. Must contain 1");
			editor.addOption("fileIdRegex", editorFileIdRegex, false);

			editorDataPath = new EditorString("./importFiles", 1, "dataPath", "The path to find the data files, must be a directory.");
			editor.addOption("dataPath", editorDataPath, false);

			editorDuration = new EditorInt(0, 99999, 1, 30 * 60, "PhenomenonTimeDuration", "Seconds between start and end phenomenonTime, in seconds.");
			editor.addOption("duration", editorDuration, true);

			editorCheckDataStream = new EditorClass<>(context, edtCtx, DsMapperFilter.class, "Check Datastream", "The Datastream to check the observations of, if the file is already imported.");
			editor.addOption("checkDs", editorCheckDataStream, true);

			editorFastCheck = new EditorBoolean(true, "Fast Check", "Do not check for each file if it is imported, only request the highest file nr and use that.");
			editor.addOption("fastCheck", editorFastCheck, true);

			editorSkipLast = new EditorBoolean(false, "Skip Last File", "Skip the last file?");
			editor.addOption("skipLast", editorSkipLast, true);

			editorSleep = new EditorInt(0, 99999, 1, 0, "Sleep", "Sleep this many seconds after every imported file.");
			editor.addOption("sleep", editorSleep, true);
		}
		return editor;
	}

	@Override
	public Iterator<List<Observation>> iterator() {
		try {
			ObsListIter obsListIter = new ObsListIter();
			return obsListIter;
		} catch (ImportException exc) {
			LOGGER.error("Failed", exc);
			throw new IllegalStateException("Failed to handle csv file.", exc);
		}
	}

	private boolean alreadyImported(String fileBase, long fileId) throws ImportException {
		if (checkDataStream == null) {
			return false;
		}
		try {
			if (fastcheck) {
				if (fastCheckLastId == null) {
					Datastream ds = checkDataStream.getDatastreamFor(null);
					Observation last = ds.observations()
							.query()
							.filter("parameters/" + TAG_IMPORT_FILE_BASE + " eq '" + Utils.escapeForStringConstant(fileBase) + "'")
							.orderBy("(parameters/" + TAG_IMPORT_FILE_ID + " add 0) desc")
							.select("parameters")
							.first();
					if (last == null) {
						fastCheckLastId = Integer.MIN_VALUE;
						return false;
					}
					fastCheckLastId = Integer.parseInt(last.getParameters().get(TAG_IMPORT_FILE_ID).toString());
				}
				return fileId <= fastCheckLastId;
			}

			Datastream ds = checkDataStream.getDatastreamFor(null);
			EntityList<Observation> list = ds.observations()
					.query()
					.filter("parameters/" + TAG_IMPORT_FILE_ID + " eq " + fileId + " and parameters/" + TAG_IMPORT_FILE_BASE + " eq '" + Utils.escapeForStringConstant(fileBase) + "'")
					.select("@iot.id").list();
			return list.size() > 0;
		} catch (ServiceFailureException exc) {
			throw new ImportException(exc);
		}
	}

	private class ObsListIter extends AbstractIterator<List<Observation>> {

		Iterator<Map.Entry<String, TreeMap<Long, File>>> baseIterator;
		Iterator<Map.Entry<Long, File>> filesIterator;
		Instant previousEndTime;
		String currentBase;

		public ObsListIter() throws ImportException {
			TreeMap<String, TreeMap<Long, File>> filesMap = fetchFiles();
			baseIterator = filesMap.entrySet().iterator();
		}

		private TreeMap<String, TreeMap<Long, File>> fetchFiles() throws ImportException {
			String targetPath = editorDataPath.getValue();
			File filesPath = new File(targetPath);
			LOGGER.info("Loading files from: {}", filesPath.getAbsolutePath());
			if (!filesPath.isDirectory()) {
				throw new ImportException("Path must be a directory.");
			}
			File[] dataFiles = filesPath.listFiles();

			Pattern idPattern = Pattern.compile(editorFileIdRegex.getValue());
			TreeMap<String, TreeMap<Long, File>> dataFilesMap = new TreeMap<>();
			for (File dataFile : dataFiles) {
				String fileName = dataFile.getName();
				Matcher idMatcher = idPattern.matcher(fileName);
				if (!idMatcher.find()) {
					LOGGER.error("File name {} does not match pattern {}", fileName, editorFileIdRegex.getValue());
					continue;
				}
				String fileBase = idMatcher.group(1);
				String idString = idMatcher.group(2);
				long fileId = Long.parseLong(idString);
				TreeMap<Long, File> idMap = dataFilesMap.get(fileBase);
				if (idMap == null) {
					idMap = new TreeMap<>();
					dataFilesMap.put(fileBase, idMap);
				}
				idMap.put(fileId, dataFile);
			}
			if (skipLast) {
				for (Map.Entry<String, TreeMap<Long, File>> baseEntry : dataFilesMap.entrySet()) {
					TreeMap<Long, File> idMap = baseEntry.getValue();
					Long lastKey = idMap.lastKey();
					idMap.remove(lastKey);
					LOGGER.info("Remove last file from list:{} - {}.", baseEntry.getKey(), lastKey);
				}
			}
			return dataFilesMap;
		}

		private List<Observation> compute() throws IOException, ImportException {
			Map.Entry<Long, File> entry = filesIterator.next();
			long fileId = entry.getKey();
			File dataFile = entry.getValue();

			Instant endTime = Instant.ofEpochMilli(dataFile.lastModified());
			if (previousEndTime == null) {
				previousEndTime = endTime.minus(phenDurationDefault, ChronoUnit.MINUTES);
				LOGGER.info("Assuming start time for file {}, as it is the first, so we have no start time.", dataFile.getName());
			}
			Instant startTime = previousEndTime;
			previousEndTime = endTime;

			if (alreadyImported(currentBase, fileId)) {
				return Collections.EMPTY_LIST;
			}

			if (sleepTime > 0) {
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException ex) {
					LOGGER.warn("Rude wakeup.", ex);
				}
			}

			LOGGER.info("Reading file id: {}, base: {}, name: {}, from {}, to {}", fileId, currentBase, dataFile.getName(), startTime, endTime);
			String data = FileUtils.readFileToString(dataFile, "UTF-8");

			List<Observation> observations = docParser.process(null, data);
			LOGGER.info("Generated {} observations.", observations.size());
			TimeObject phenTime = new TimeObject(Interval.of(startTime, endTime));

			Map<String, Object> parameters = new HashMap<>();
			parameters.put(TAG_IMPORT_FILE_ID, fileId);
			parameters.put(TAG_IMPORT_FILE_BASE, currentBase);
			for (Observation obs : observations) {
				obs.setPhenomenonTime(phenTime);
				obs.setParameters(parameters);
				parameters.put("resultCount", getResultCount(obs));
			}
			return observations;
		}

		private int getResultCount(Observation obs) {
			Object result = obs.getResult();
			if (result instanceof List) {
				return ((List) result).size();
			}
			return 1;
		}

		@Override
		protected List<Observation> computeNext() {
			if (filesIterator == null || !filesIterator.hasNext()) {
				if (baseIterator.hasNext()) {
					Map.Entry<String, TreeMap<Long, File>> nextBaseEntry = baseIterator.next();
					filesIterator = nextBaseEntry.getValue().entrySet().iterator();
					currentBase = nextBaseEntry.getKey();
				} else {
					return endOfData();
				}
			}
			if (filesIterator.hasNext()) {
				try {
					return compute();
				} catch (ImportException | IOException exc) {
					LOGGER.error("Failed", exc);
				}
			}
			return endOfData();
		}
	}

}
