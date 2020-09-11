/*
 * Copyright (C) 2020 Fraunhofer IOSB
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
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.util.List;
import org.apache.commons.csv.CSVRecord;

/**
 *
 * @author hylke
 */
public interface RecordConverter {

	public default void init(SensorThingsService service) throws ImportException {
	}

	public default void setVerbose(boolean verbose) {
	}

	public List<Observation> convert(CSVRecord record) throws ImportException;
}
