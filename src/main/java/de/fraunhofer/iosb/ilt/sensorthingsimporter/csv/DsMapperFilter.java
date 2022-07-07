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
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator.StringType;
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
public class DsMapperFilter implements DatastreamMapper, AnnotatedConfigurable<SensorThingsService, Object> {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(DsMapperFilter.class);
	private final Map<String, Datastream> datastreamCache = new HashMap<>();
	private final Map<String, MultiDatastream> multiDatastreamCache = new HashMap<>();

	@ConfigurableField(editor = EditorString.class,
			label = "Filter", description = "A filter that will be added to the query for the datastream.\nUse placeholders {colNr} to add the content of columns to the query.")
	@EditorString.EdOptsString(dflt = "Thing/properties/id eq {1}", lines = 3)
	private String filterTemplate;

	@ConfigurableField(editor = EditorSubclass.class, optional = true,
			label = "DS Generator", description = "Generates Datastreams if the required one does not exist yet.")
	@EditorSubclass.EdOptsSubclass(iface = DatastreamGenerator.class)
	private DatastreamGenerator dsGenerator;

	private SensorThingsService service;

	public DsMapperFilter() {
	}

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx, ConfigEditor<?> configEditor) throws ConfigurationException {
		service = context;
		AnnotatedConfigurable.super.configure(config, context, edtCtx, configEditor);
	}

	@Override
	public Datastream getDatastreamFor(CSVRecord record) throws ImportException {
		try {
			String filter = Translator.fillTemplate(filterTemplate, record, StringType.URL, true);
			Datastream ds = getDatastreamFor(filter, record);
			return ds;
		} catch (ServiceFailureException ex) {
			LOGGER.error("Failed to fetch datastream.", ex);
			throw new IllegalArgumentException(ex);
		}
	}

	@Override
	public MultiDatastream getMultiDatastreamFor(CSVRecord record) {
		try {
			String filter = Translator.fillTemplate(filterTemplate, record, StringType.URL, true);
			MultiDatastream ds = getMultiDatastreamFor(filter, record);
			return ds;
		} catch (ServiceFailureException ex) {
			LOGGER.error("Failed to fetch datastream.", ex);
			throw new IllegalArgumentException(ex);
		}
	}

	private Datastream getDatastreamFor(String filter, CSVRecord record) throws ServiceFailureException, ImportException {
		Datastream ds = datastreamCache.get(filter);
		if (ds != null) {
			return ds;
		}
		if (datastreamCache.containsKey(filter)) {
			// We previously had found nothing. Don't search again.
			return null;
		}
		Query<Datastream> query = service.datastreams().query().filter(filter);
		EntityList<Datastream> streams = query.list();
		if (streams.size() > 1) {
			LOGGER.error("Found incorrect number of datastreams: {} for filter: {}", streams.size(), filter);
			throw new ImportException("Found incorrect number of datastreams: " + streams.size() + " for filter: " + filter);
		} else if (streams.isEmpty()) {
			if (dsGenerator != null) {
				ds = dsGenerator.createDatastreamFor(record);
				if (ds != null) {
					LOGGER.info("Created datastream {} for filter {}.", ds, filter);
				}
			}
		} else {
			ds = streams.iterator().next();
			LOGGER.debug("Found datastream {} for filter {}.", ds.getId(), filter);
		}
		if (ds == null) {
			LOGGER.error("Found no datastreams for filter: {}.", filter);
		}
		datastreamCache.put(filter, ds);
		return ds;
	}

	private MultiDatastream getMultiDatastreamFor(String filter, CSVRecord record) throws ServiceFailureException {
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
