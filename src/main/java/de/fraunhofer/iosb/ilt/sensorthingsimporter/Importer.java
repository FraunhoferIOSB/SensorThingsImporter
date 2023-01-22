/*
 * Copyright (C) 2017 Fraunhofer Institut IOSB, Fraunhoferstr. 1, D 76131
 * Karlsruhe, Germany.
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
package de.fraunhofer.iosb.ilt.sensorthingsimporter;

import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ProgressTracker;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import java.util.List;

/**
 *
 * @author scf
 */
public interface Importer extends Iterable<List<Observation>> {

	/**
	 * Tell the importer to give lots of output.
	 *
	 * @param verbose flag indicating that the importer should give lots of
	 * output.
	 */
	public default void setVerbose(boolean verbose) {
		// does nothing by default
	}

	public default void setNoAct(boolean noAct) {
		// does nothing by default
	}

	public default void setProgressTracker(ProgressTracker tracker) {
		// does nothing by default
	}

	public default String getErrorLog() {
		return "";
	}
}
