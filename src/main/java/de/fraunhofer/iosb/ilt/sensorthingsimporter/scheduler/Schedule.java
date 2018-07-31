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

import de.fraunhofer.iosb.ilt.configurable.AbstractConfigurable;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;

/**
 *
 * @author scf
 */
public class Schedule extends AbstractConfigurable<Void, Void> {

	@ConfigurableField(editor = EditorString.class,
			label = "Cron",
			description = "A cron-compatible scheduling definition. See http://www.quartz-scheduler.org/api/2.2.1/org/quartz/CronExpression.html")
	@EditorString.EdOptsString(dflt = "0 15 10 * * ?")
	private String cronLine;

	@ConfigurableField(editor = EditorString.class,
			label = "File",
			description = "The ImporterConfig for this cron job.")
	@EditorString.EdOptsString(dflt = "importer.json")
	private String fileName;

	/**
	 * @return the cronLine
	 */
	public String getCronLine() {
		return cronLine;
	}

	/**
	 * @param cronLine the cronLine to set
	 */
	public void setCronLine(String cronLine) {
		this.cronLine = cronLine;
	}

	/**
	 * @return the fileName
	 */
	public String getFileName() {
		return fileName;
	}

	/**
	 * @param fileName the fileName to set
	 */
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
}
