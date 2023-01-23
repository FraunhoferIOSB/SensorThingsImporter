/*
 * Copyright (C) 2023 Fraunhofer IOSB
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

import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.sta.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hylke
 */
public class ErrorLog implements AnnotatedConfigurable<Object, Object> {

	private static final Logger LOGGER = LoggerFactory.getLogger(ErrorLog.class.getName());

	public static final int DEFAULT_MAX_FILES_PER_TYPE = 20;
	public static final int DEFAULT_MAX_LINES_PER_FILE = 5;

	@ConfigurableField(editor = EditorInt.class,
			label = "Max Files", description = "Max # of files to track errors for")
	@EditorInt.EdOptsInt(dflt = DEFAULT_MAX_FILES_PER_TYPE, max = 999)
	private int maxFiles;

	@ConfigurableField(editor = EditorInt.class,
			label = "Max Lines", description = "Max # of lines per file to track errors for")
	@EditorInt.EdOptsInt(dflt = DEFAULT_MAX_FILES_PER_TYPE, max = 999)
	private int maxLines;

	private Map<String, ErrorType> errorTypes = new HashMap<>();
	private String currentFileName = "";
	private int currentLine;
	private int errorCount = 0;

	public ErrorLog() {
	}

	public ErrorLog(int maxFiles, int maxLines) {
		this.maxFiles = maxFiles;
		this.maxLines = maxLines;
	}

	public void addError(String type) {
		addError(type, currentFileName, currentLine);
	}

	public synchronized void addError(String type, String fileName, int line) {
		ErrorType errorType = errorTypes.computeIfAbsent(type, (t) -> {
			return new ErrorType(t, maxFiles, maxLines);
		});
		errorType.addErrorInFile(fileName, line);
		errorCount++;
	}

	public synchronized void clear() {
		errorTypes.clear();
		errorCount = 0;
	}

	public String getCurrentFileName() {
		return currentFileName;
	}

	public void setCurrentFileName(String currentFileName) {
		this.currentFileName = currentFileName;
	}

	public int getCurrentLine() {
		return currentLine;
	}

	public void setCurrentLine(int currentLine) {
		this.currentLine = currentLine;
	}

	public void logErrors() {
		if (errorCount > 0) {
			LOGGER.info(getErrors());
		}
	}

	public String getErrors() {
		var sb = new StringBuilder();
		for (var errorType : errorTypes.values()) {
			sb.append(errorType.getType())
					.append('\n');
			for (var errorFile : errorType.getFiles().values()) {
				sb.append("  ")
						.append(Utils.cleanForLogging(errorFile.getFileName(), 200))
						.append(": ")
						.append(errorFile.getLines().toString())
						.append('\n');
			}
		}
		return sb.toString();
	}

	public int getErrorCount() {
		return errorCount;
	}

	public static class ErrorFile {

		private final String fileName;
		private final List<Integer> lines = new ArrayList<>();
		private final int maxLines;

		public ErrorFile(String fileName) {
			this(fileName, DEFAULT_MAX_LINES_PER_FILE);
		}

		public ErrorFile(String fileName, int maxLines) {
			this.fileName = fileName;
			this.maxLines = maxLines;
		}

		public String getFileName() {
			return fileName;
		}

		public List<Integer> getLines() {
			return lines;
		}

		public void addLine(int line) {
			if (lines.size() < maxLines) {
				lines.add(line);
			}
		}
	}

	public static class ErrorType {

		private final String type;
		private final Map<String, ErrorFile> errorFiles = new HashMap<>();
		private final int maxFiles;
		private final int maxLines;

		public ErrorType(String type) {
			this(type, DEFAULT_MAX_FILES_PER_TYPE, DEFAULT_MAX_LINES_PER_FILE);
		}

		public ErrorType(String type, int maxFiles, int maxLines) {
			this.type = type;
			this.maxFiles = maxFiles;
			this.maxLines = maxLines;
		}

		public String getType() {
			return type;
		}

		public void addErrorInFile(String fileName, int line) {
			ErrorFile file = errorFiles.get(fileName);
			if (file == null && errorFiles.size() >= maxFiles) {
				return;
			}
			if (file == null) {
				file = new ErrorFile(fileName, maxLines);
				errorFiles.put(fileName, file);
			}
			file.addLine(line);
		}

		public Map<String, ErrorFile> getFiles() {
			return errorFiles;
		}
	}
}
