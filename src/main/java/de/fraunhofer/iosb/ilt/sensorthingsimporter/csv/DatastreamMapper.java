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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.csv;

import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import org.apache.commons.csv.CSVRecord;

/**
 *
 * @author scf
 */
public interface DatastreamMapper {

	/**
	 * Get the Datastream to be used for the given record.
	 *
	 * @param record The record to get the Datastream for.
	 * @return The Datastream to use for the given record.
	 * @throws ImportException if there is a permanent failure.
	 */
	public Datastream getDatastreamFor(CSVRecord record) throws ImportException;

	public MultiDatastream getMultiDatastreamFor(CSVRecord record) throws ImportException;
}
