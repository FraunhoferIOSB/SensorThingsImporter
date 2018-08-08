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
import de.fraunhofer.iosb.ilt.configurable.AbstractConfigurable;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorClass;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorList;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Options;
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
public class ImporterScheduler extends AbstractConfigurable<Void, Void> {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ImporterScheduler.class);

	@ConfigurableField(editor = EditorList.class,
			label = "Schedules",
			description = "The schedules to schedule.")
	@EditorList.EdOptsList(editor = EditorClass.class)
	@EditorClass.EdOptsClass(clazz = Schedule.class)
	private List<Schedule> schedules;

	private boolean noAct = false;
	private Scheduler scheduler;

	private Thread shutdownHook;

	public void loadOptions(Options options) throws IOException {
		noAct = options.getNoAct().isSet();
		String fileName = options.getFileName().getValue();
		File file = new File(fileName);
		String config = FileUtils.readFileToString(file, "UTF-8");
		setConfig(config);
	}

	public void setConfig(String config) {
		JsonElement json = new JsonParser().parse(config);
		configure(json, null, null);
	}

	public void start() throws SchedulerException {
		scheduler = StdSchedulerFactory.getDefaultScheduler();
		addShutdownHook();
		scheduler.start();

		int i = 0;
		for (final Schedule schedule : schedules) {
			String triggerName = "trigger" + i;
			String jobName = "job" + i;

			JobDetail jobDetail = JobBuilder.newJob(ImporterJob.class)
					.withIdentity(jobName)
					.usingJobData(ImporterJob.KEY_FILENAME, schedule.getFileName())
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

}