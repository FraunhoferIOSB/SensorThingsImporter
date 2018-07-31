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
import java.io.File;
import java.io.IOException;
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
	public static final String KEY_NO_ACT = "noAct";

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		try {
			JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
			String importerFileName = jobDataMap.getString(KEY_FILENAME);
			boolean noAct = jobDataMap.getBooleanValue(KEY_NO_ACT);

			String config = loadFile(importerFileName);
			ImporterWrapper importer = new ImporterWrapper();
			importer.doImport(config, noAct);
		} catch (IOException ex) {
			LOGGER.error("Failed to load configuration.", ex);
			throw new JobExecutionException("Failed to load configuration", ex);
		}
	}

	public static String loadFile(String fileName) throws IOException {
		File file = new File(fileName);
		String config = FileUtils.readFileToString(file, "UTF-8");
		return config;
	}

}
