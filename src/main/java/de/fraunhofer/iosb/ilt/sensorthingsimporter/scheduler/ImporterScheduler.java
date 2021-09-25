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

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.ConfigurationException;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorClass;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorList;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorLong;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Options;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ChangingStatusLogger;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class ImporterScheduler implements AnnotatedConfigurable<Void, Void> {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ImporterScheduler.class);

	public static final long DEFAULT_LOG_INTERVAL = 10000;
	public static final ChangingStatusLogger STATUS_LOGGER = new ChangingStatusLogger(LOGGER).setLogIntervalMs(DEFAULT_LOG_INTERVAL);

	@ConfigurableField(editor = EditorList.class,
			label = "Schedules",
			description = "The schedules to schedule.")
	@EditorList.EdOptsList(editor = EditorClass.class)
	@EditorClass.EdOptsClass(clazz = Schedule.class)
	private List<Schedule> schedules;

	@ConfigurableField(editor = EditorLong.class, optional = true,
			label = "LogInterval",
			description = "Delay in ms between log messages")
	@EditorLong.EdOptsLong(dflt = DEFAULT_LOG_INTERVAL)
	private long logInterval = DEFAULT_LOG_INTERVAL;

	private boolean noAct = false;
	private Scheduler scheduler;
	private File basePath;

	private Thread shutdownHook;

	public void loadOptions(Options options) throws IOException, ConfigurationException {
		noAct = options.getNoAct().isSet();
		String fileName = options.getFileName().getValue();
		LOGGER.info("Loading schedule from {}", fileName);
		File file = new File(fileName);
		basePath = file.getAbsoluteFile().getParentFile();
		LOGGER.info("Setting base path to {}", basePath);
		String config = FileUtils.readFileToString(file, "UTF-8");
		setConfig(config);
	}

	public void setConfig(String config) throws ConfigurationException {
		JsonElement json = JsonParser.parseString(config);
		configure(json, null, null, null);
	}

	public void start() throws SchedulerException {
		scheduler = StdSchedulerFactory.getDefaultScheduler();
		addShutdownHook();
		scheduler.start();
		STATUS_LOGGER.setLogIntervalMs(logInterval);
		STATUS_LOGGER.start();

		int i = 0;
		for (final Schedule schedule : schedules) {
			String triggerName = "trigger" + i;
			String jobName = "job" + i;
			File file = new File(basePath, schedule.getFileName());
			String fileName = file.getAbsolutePath();
			LOGGER.info("Adding job {} with schedule {} from file {}", jobName, schedule.getCronLine(), fileName);

			JobDetail jobDetail = JobBuilder.newJob(ImporterJob.class)
					.withIdentity(jobName)
					.usingJobData(ImporterJob.KEY_FILENAME, fileName)
					.usingJobData(ImporterJob.KEY_SHELLSCRIPT, schedule.isShellScript())
					.usingJobData(ImporterJob.KEY_NO_ACT, noAct)
					.build();

			CronTrigger trigger = TriggerBuilder.newTrigger()
					.withIdentity(triggerName)
					.withSchedule(CronScheduleBuilder.cronSchedule(schedule.getCronLine()))
					.build();

			scheduler.scheduleJob(jobDetail, trigger);
			i++;
		}

	}

	private synchronized void addShutdownHook() {
		if (this.shutdownHook == null) {
			this.shutdownHook = new Thread(() -> {
				LOGGER.info("Shutting down...");
				STATUS_LOGGER.stop();
				try {
					if (scheduler != null) {
						scheduler.shutdown();
					}
				} catch (SchedulerException ex) {
					LOGGER.warn("Exception stopping scheduler.", ex);
				}
			});
			Runtime.getRuntime().addShutdownHook(shutdownHook);
		}
	}

	/**
	 * @return the noAct
	 */
	public boolean isNoAct() {
		return noAct;
	}

	/**
	 * @param noAct the noAct to set
	 */
	public void setNoAct(boolean noAct) {
		this.noAct = noAct;
	}

	public long getLogInterval() {
		return logInterval;
	}

	public void setLogInterval(long logInterval) {
		this.logInterval = logInterval;
	}

}
