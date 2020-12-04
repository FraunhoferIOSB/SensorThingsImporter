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

import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.ConfigurationException;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.FrostUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.JsonUtils;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import de.fraunhofer.iosb.ilt.sta.model.Sensor;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.model.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.sta.query.Query;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hylke
 */
public class DataStreamGeneratorNames implements DatastreamGenerator, AnnotatedConfigurable<SensorThingsService, Object> {

	private static final Logger LOGGER = LoggerFactory.getLogger(DataStreamGeneratorNames.class.getName());

	@ConfigurableField(editor = EditorString.class,
			label = "Thing Filter", description = "A filter that will be used to find the Thing for the Datastream to create.")
	@EditorString.EdOptsString(dflt = "name eq {nameColumn}", lines = 3)
	private String filterThing;

	@ConfigurableField(editor = EditorString.class,
			label = "Sensor Filter", description = "A filter that will be used to find the Sensor for the Datastream to create.")
	@EditorString.EdOptsString(dflt = "name eq {nameColumn}", lines = 3)
	private String filterSensor;

	@ConfigurableField(editor = EditorString.class,
			label = "ObsProp Filter", description = "A filter that will be used to find the Sensor for the Datastream to create.")
	@EditorString.EdOptsString(dflt = "name eq {nameColumn}", lines = 3)
	private String filterObsProp;

	@ConfigurableField(editor = EditorString.class,
			label = "Name", description = "The name")
	@EditorString.EdOptsString(dflt = "{nameColumn}", lines = 1)
	private String templateName;

	@ConfigurableField(editor = EditorString.class,
			label = "Description", description = "The description")
	@EditorString.EdOptsString(dflt = "{descColumn}", lines = 1)
	private String templateDescription;

	@ConfigurableField(editor = EditorString.class,
			label = "Properties", description = "Template used to generate properties.")
	@EditorString.EdOptsString(lines = 4)
	private String templateProperties;

	@ConfigurableField(editor = EditorString.class,
			label = "UoM Name", description = "The name of the Unit Of Measurement")
	@EditorString.EdOptsString(dflt = "{column}", lines = 1)
	private String templateUomName;

	@ConfigurableField(editor = EditorString.class,
			label = "UoM Symbol", description = "The symbol of the Unit Of Measurement")
	@EditorString.EdOptsString(dflt = "{column}", lines = 1)
	private String templateUomSymbol;

	@ConfigurableField(editor = EditorString.class,
			label = "UoM Definition", description = "The definition of the Unit Of Measurement")
	@EditorString.EdOptsString(dflt = "{column}", lines = 1)
	private String templateUomDef;

	@ConfigurableField(editor = EditorString.class,
			label = "Obs Type", description = "The type of observations")
	@EditorString.EdOptsString(dflt = "\"http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement", lines = 1)
	private String templateObsType;

	private final Map<String, Thing> cacheThings = new HashMap<>();
	private final Map<String, Sensor> cacheSensors = new HashMap<>();
	private final Map<String, ObservedProperty> cacheObsProps = new HashMap<>();

	private SensorThingsService service;
	private FrostUtils frostUtils;

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx, ConfigEditor<?> configEditor) throws ConfigurationException {
		service = context;
		frostUtils = new FrostUtils(context);
		AnnotatedConfigurable.super.configure(config, context, edtCtx, configEditor);
	}

	@Override
	public Datastream createDatastreamFor(CSVRecord record) throws ImportException {
		Thing thing = getThingFor(record);
		Sensor sensor = getSensorFor(record);
		ObservedProperty obsProp = getObsPropFor(record);
		if (thing == null || sensor == null || obsProp == null) {
			return null;
		}
		Datastream ds = new Datastream();
		ds.setName(Translator.fillTemplate(templateName, record, false, false, true));
		ds.setDescription(Translator.fillTemplate(templateDescription, record, false, false, true));
		ds.setObservationType(Translator.fillTemplate(templateObsType, record, false, false, true));
		String propertiesString = Translator.fillTemplate(templateProperties, record, false, true, false);
		ds.setProperties(JsonUtils.jsonToMap(propertiesString));
		UnitOfMeasurement uom = new UnitOfMeasurement(
				Translator.fillTemplate(templateUomName, record, false, false, true),
				Translator.fillTemplate(templateUomSymbol, record, false, false, true),
				Translator.fillTemplate(templateUomDef, record, false, false, true)
		);
		ds.setUnitOfMeasurement(uom);
		ds.setThing(thing);
		ds.setSensor(sensor);
		ds.setObservedProperty(obsProp);
		try {
			frostUtils.create(ds);
		} catch (ServiceFailureException ex) {
			throw new ImportException("Failed to create Datastream", ex);
		}
		return ds;
	}

	public Thing getThingFor(CSVRecord record) throws ImportException {
		try {
			String filter = Translator.fillTemplate(filterThing, record, true, false, true);
			Thing t = getThingFor(filter);
			return t;
		} catch (ServiceFailureException ex) {
			LOGGER.error("Failed to fetch datastream.", ex);
			throw new IllegalArgumentException(ex);
		}
	}

	public Sensor getSensorFor(CSVRecord record) throws ImportException {
		try {
			String filter = Translator.fillTemplate(filterSensor, record, true, false, true);
			Sensor s = getSensorFor(filter);
			return s;
		} catch (ServiceFailureException ex) {
			LOGGER.error("Failed to fetch datastream.", ex);
			throw new IllegalArgumentException(ex);
		}
	}

	public ObservedProperty getObsPropFor(CSVRecord record) throws ImportException {
		try {
			String filter = Translator.fillTemplate(filterObsProp, record, true, false, true);
			ObservedProperty o = getObsPropFor(filter);
			return o;
		} catch (ServiceFailureException ex) {
			LOGGER.error("Failed to fetch datastream.", ex);
			throw new IllegalArgumentException(ex);
		}
	}

	private Thing getThingFor(String filter) throws ServiceFailureException, ImportException {
		Thing t = cacheThings.get(filter);
		if (t != null) {
			return t;
		}
		if (cacheThings.containsKey(filter)) {
			// We previously had found nothing. Don't search again.
			return null;
		}
		Query<Thing> query = service.things().query().filter(filter);
		EntityList<Thing> streams = query.list();
		if (streams.size() > 1) {
			LOGGER.error("Found incorrect number of Things: {} for filter: {}", streams.size(), filter);
			throw new ImportException("Found incorrect number of Things: " + streams.size() + " for filter: " + filter);
		} else if (!streams.isEmpty()) {
			t = streams.iterator().next();
			LOGGER.debug("Found Thing {} for filter {}.", t.getId(), filter);
		}
		if (t == null) {
			LOGGER.error("Found no Thing for filter: {}.", filter);
		}
		cacheThings.put(filter, t);
		return t;
	}

	private Sensor getSensorFor(String filter) throws ServiceFailureException, ImportException {
		Sensor s = cacheSensors.get(filter);
		if (s != null) {
			return s;
		}
		if (cacheSensors.containsKey(filter)) {
			// We previously had found nothing. Don't search again.
			return null;
		}
		Query<Sensor> query = service.sensors().query().filter(filter);
		EntityList<Sensor> streams = query.list();
		if (streams.size() > 1) {
			LOGGER.error("Found incorrect number of Sensors: {} for filter: {}", streams.size(), filter);
			throw new ImportException("Found incorrect number of Sensors: " + streams.size() + " for filter: " + filter);
		} else if (!streams.isEmpty()) {
			s = streams.iterator().next();
			LOGGER.debug("Found Sensor {} for filter {}.", s.getId(), filter);
		}
		if (s == null) {
			LOGGER.error("Found no Sensor for filter: {}.", filter);
		}
		cacheSensors.put(filter, s);
		return s;
	}

	private ObservedProperty getObsPropFor(String filter) throws ServiceFailureException, ImportException {
		ObservedProperty o = cacheObsProps.get(filter);
		if (o != null) {
			return o;
		}
		if (cacheObsProps.containsKey(filter)) {
			// We previously had found nothing. Don't search again.
			return null;
		}
		Query<ObservedProperty> query = service.observedProperties().query().filter(filter);
		EntityList<ObservedProperty> streams = query.list();
		if (streams.size() > 1) {
			LOGGER.error("Found incorrect number of ObservedProperties: {} for filter: {}", streams.size(), filter);
			throw new ImportException("Found incorrect number of ObservedProperties: " + streams.size() + " for filter: " + filter);
		} else if (!streams.isEmpty()) {
			o = streams.iterator().next();
			LOGGER.debug("Found ObservedProperties {} for filter {}.", o.getId(), filter);
		}
		if (o == null) {
			LOGGER.error("Found no ObservedProperties for filter: {}.", filter);
		}
		cacheObsProps.put(filter, o);
		return o;
	}

}
