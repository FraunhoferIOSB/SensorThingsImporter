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

import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.query.Query;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class DsMapperFilter implements DatastreamMapper {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(DsMapperFilter.class);
	private final Map<String, Datastream> datastreamCache = new HashMap<>();
	private final Map<String, MultiDatastream> multiDatastreamCache = new HashMap<>();

	private SensorThingsService service;
	private String filterTemplate;

	private EditorString editor;

	public DsMapperFilter() {
	}

	@Override
	public void configure(JsonElement config, Object context, Object edtCtx) {
		if (!(context instanceof SensorThingsService)) {
			throw new IllegalArgumentException("Context must be a SensorThingsService. We got a " + context.getClass());
		}
		service = (SensorThingsService) context;
		getConfigEditor(service, edtCtx).setConfig(config);
		filterTemplate = editor.getValue();
	}

	@Override
	public EditorString getConfigEditor(Object context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorString("Thing/properties/id eq {1}", 3, "Filter",
					"A filter that will be added to the query for the datastream."
					+ " Use placeholders {colNr} to add the content of columns to the query.");
		}
		return editor;
	}

	@Override
	public Datastream getDatastreamFor(CSVRecord record) {
		try {
			String filter = Translator.fillTemplate(filterTemplate, record);
			Datastream ds = getDatastreamFor(filter);
			return ds;
		} catch (ServiceFailureException ex) {
			LOGGER.error("Failed to fetch datastream.", ex);
			throw new IllegalArgumentException(ex);
		}
	}

	@Override
	public MultiDatastream getMultiDatastreamFor(CSVRecord record) {
		try {
			String filter = Translator.fillTemplate(filterTemplate, record);
			MultiDatastream ds = getMultiDatastreamFor(filter);
			return ds;
		} catch (ServiceFailureException ex) {
			LOGGER.error("Failed to fetch datastream.", ex);
			throw new IllegalArgumentException(ex);
		}
	}

	private Datastream getDatastreamFor(String filter) throws ServiceFailureException {
		Datastream ds = datastreamCache.get(filter);
		if (ds != null) {
			return ds;
		}
		Query<Datastream> query = service.datastreams().query().filter(filter);
		EntityList<Datastream> streams = query.list();
		if (streams.size() != 1) {
			LOGGER.error("Found incorrect number of datastreams: {} for filter: {}", streams.size(), filter);
			throw new IllegalArgumentException("Found incorrect number of datastreams: " + streams.size());
		}
		ds = streams.iterator().next();
		LOGGER.info("Found datastream {} for query {}.", ds.getId(), filter);
		datastreamCache.put(filter, ds);
		return ds;
	}

	private MultiDatastream getMultiDatastreamFor(String filter) throws ServiceFailureException {
		MultiDatastream mds = multiDatastreamCache.get(filter);
		if (mds != null) {
			return mds;
		}
		Query<MultiDatastream> query = service.multiDatastreams().query().filter(filter);
		EntityList<MultiDatastream> streams = query.list();
		if (streams.size() != 1) {
			LOGGER.error("Found incorrect number of multiDatastreams: {}", streams.size());
			throw new IllegalArgumentException("Found incorrect number of multiDatastreams: " + streams.size());
		}
		mds = streams.iterator().next();
		LOGGER.info("Found multiDatastreams {} for query {}.", mds.getId(), filter);
		multiDatastreamCache.put(filter, mds);
		return mds;
	}

}
