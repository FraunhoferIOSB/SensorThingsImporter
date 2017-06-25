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

import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A datastream mapper that always returns the same Datastream.
 *
 * @author scf
 */
public class DsMapperFixed implements DatastreamMapper {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(DsMapperFixed.class);

	private SensorThingsService service;

	private Datastream ds;

	private EditorInt editor;

	public DsMapperFixed() {
		this.ds = null;
	}

	@Override
	public void configure(JsonElement config, Object context, Object edtCtx) {
		if (!(context instanceof SensorThingsService)) {
			throw new IllegalArgumentException("Context must be a SensorThingsService. We got a " + context.getClass());
		}
		service = (SensorThingsService) context;
		getConfigEditor(service, edtCtx).setConfig(config, service, edtCtx);
	}

	private void init() {
		long dsId = editor.getValue();
		try {
			ds = service.datastreams().find(dsId);
			LOGGER.info("Using fixed datatsream: {}", ds.getName());
		} catch (ServiceFailureException exc) {
			throw new IllegalArgumentException("Could not fetch datastream for id " + dsId, exc);
		}
	}

	@Override
	public EditorInt getConfigEditor(Object context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorInt(Integer.MIN_VALUE, Integer.MAX_VALUE, 1, 0, "Datastream ID", "The datastream id to add the observations to.");
		}
		return editor;
	}

	@Override
	public Datastream getDatastreamFor(CSVRecord record) {
		if (ds == null) {
			init();
		}
		return ds;
	}

}
