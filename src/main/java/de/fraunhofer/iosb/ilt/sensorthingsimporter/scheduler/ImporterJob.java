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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.scheduler;

import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImporterWrapper;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Options;
import de.fraunhofer.iosb.ilt.sta.Utils;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.commons.io.FileUtils;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
@DisallowConcurrentExecution
public class ImporterJob implements Job {

	/**
	 * The logger for this class.
	 */
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ImporterJob.class);
	public static final String KEY_FILENAME = "fileName";
	public static final String KEY_SHELLSCRIPT = "shellScript";
	public static final String KEY_NO_ACT = "noAct";

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		String importerFileName = "unknown";
		try {
			JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
			importerFileName = jobDataMap.getString(KEY_FILENAME);
			boolean shellScript = jobDataMap.getBoolean(KEY_SHELLSCRIPT);
			boolean noAct = jobDataMap.getBooleanValue(KEY_NO_ACT);
			if (shellScript) {
				executeShellScript(importerFileName);
			} else {
				executeImport(importerFileName, noAct);
			}
		} catch (IOException ex) {
			LOGGER.error("Failed to load configuration.", ex);
			throw new JobExecutionException("Failed to load configuration", ex);
		} catch (Exception exc) {
			LOGGER.error("ImporterJob " + importerFileName + " caused an exception!", exc);
		}
	}

	private void executeShellScript(String importerFileName) throws IOException {
		LOGGER.info("Starting {}", importerFileName);
		Process process = new ProcessBuilder(importerFileName)
				.redirectErrorStream(true)
				.start();
		CompletableFuture<String> outputFuture = readInputStream(process.getInputStream());
		try {
			process.onExit().get();
			LOGGER.info("Script output:\n{}", outputFuture.get());
		} catch (InterruptedException | ExecutionException ex) {
			LOGGER.error("Exception waiting for script.", ex);
		}
	}

	private void executeImport(String importerFileName, boolean noAct) throws IOException {
		String config = loadFile(importerFileName);
		ImporterWrapper importer = new ImporterWrapper();
		importer.setName(importerFileName);
		importer.doImport(config, noAct, null);
	}

	public static String loadFile(String fileName) throws IOException {
		File file = new File(fileName);
		if (file.exists()) {
			String config = FileUtils.readFileToString(file, "UTF-8");
			return config;
		} else {
			String config = Options.getEnv(fileName, "");
			if (!Utils.isNullOrEmpty(config)) {
				return config;
			}
		}
		LOGGER.error("Could not load configuration from {}. Not a file, nor an environment variable.", fileName);
		return null;
	}

	public static CompletableFuture<String> readInputStream(InputStream is) {
		return CompletableFuture.supplyAsync(() -> {
			try (InputStreamReader isr = new InputStreamReader(is); BufferedReader br = new BufferedReader(isr)) {
				StringBuilder result = new StringBuilder();
				String inputLine;
				while ((inputLine = br.readLine()) != null) {
					result.append(inputLine).append(System.lineSeparator());
				}
				return result.toString();
			} catch (Throwable e) {
				throw new RuntimeException("Exception reading string.", e);
			}
		});
	}

}
