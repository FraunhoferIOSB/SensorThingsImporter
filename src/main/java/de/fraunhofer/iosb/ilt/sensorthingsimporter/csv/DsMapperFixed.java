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

import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.ConfigurationException;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.ErrorLog;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A datastream mapper that always returns the same Datastream.
 *
 * @author scf
 */
public class DsMapperFixed implements DatastreamMapper, AnnotatedConfigurable<SensorThingsService, Object> {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(DsMapperFixed.class);

	private SensorThingsService service;

	private Datastream ds;
	private MultiDatastream mds;

	@ConfigurableField(editor = EditorInt.class, label = "Datastream ID", description = "The datastream id to add the observations to.")
	@EditorInt.EdOptsInt()
	private int dsId;

	public DsMapperFixed() {
		this.ds = null;
	}

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx, ConfigEditor<?> configEditor) throws ConfigurationException {
		service = context;
		AnnotatedConfigurable.super.configure(config, context, edtCtx, configEditor);
	}

	private void init(boolean multi) {
		try {
			if (multi) {
				mds = service.multiDatastreams().find(dsId);
				LOGGER.info("Using fixed multiDatastream: {}", mds.getName());
			} else {
				ds = service.datastreams().find(dsId);
				LOGGER.info("Using fixed datatsream: {}", ds.getName());
			}
		} catch (ServiceFailureException exc) {
			throw new IllegalArgumentException("Could not fetch (multi)datastream for id " + dsId, exc);
		}
	}

	@Override
	public Datastream getDatastreamFor(CSVRecord record, ErrorLog errorLog) {
		if (ds == null) {
			init(false);
		}
		return ds;
	}

	@Override
	public MultiDatastream getMultiDatastreamFor(CSVRecord record, ErrorLog errorLog) {
		if (mds == null) {
			init(true);
		}
		return mds;
	}

}
