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
package de.fraunhofer.iosb.ilt.sensorthingsimporter;

import org.apache.commons.csv.CSVRecord;

import de.fraunhofer.iosb.ilt.sta.model.Datastream;

/**
 * A datastream mapper that always returns the same Datastream.
 *
 * @author scf
 */
public class DsMapperFixed implements DatastreamMapper {

    private final Datastream ds;

    public DsMapperFixed(Datastream ds) {
        this.ds = ds;
    }

    @Override
    public Datastream getDatastreamFor(CSVRecord record) {
        return ds;
    }

}
