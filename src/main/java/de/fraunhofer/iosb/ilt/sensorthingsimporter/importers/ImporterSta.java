/*
 * Copyright (C) 2022 Fraunhofer IOSB
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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.importers;

import com.google.gson.JsonElement;
import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.ConfigEditor;
import de.fraunhofer.iosb.ilt.configurable.ConfigurationException;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Importer;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.auth.AuthMethod;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.timegen.TimeGen;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.FrostUtils;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.FrostUtils.addOrCreateFilter;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.StatusCodeException;
import de.fraunhofer.iosb.ilt.sta.Utils;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

/**
 *
 * @author hylke
 */
public class ImporterSta implements Importer, AnnotatedConfigurable<SensorThingsService, Object> {

	private static final Logger LOGGER = LoggerFactory.getLogger(ImporterSta.class.getName());

	@ConfigurableField(editor = EditorString.class,
			label = "Source Service URL", description = "The url of the server to import from.")
	@EditorString.EdOptsString(dflt = "http://localhost:8080/FROST-Server/v1.0")
	private String sourceServiceUrl;

	@ConfigurableField(editor = EditorString.class, optional = true,
			label = "Replace URL", description = "The part of server generated URLs that needs to be replaced with the Service URL.")
	@EditorString.EdOptsString(dflt = "")
	private String serviceUrlReplace;

	@ConfigurableField(editor = EditorSubclass.class,
			label = "Source Auth Method", description = "The authentication method the service uses.",
			optional = true)
	@EditorSubclass.EdOptsSubclass(iface = AuthMethod.class)
	private AuthMethod sourceAuthMethod;

	@ConfigurableField(editor = EditorInt.class,
			label = "Days Per Request", description = "Request Observations for this many days at a time.")
	@EditorInt.EdOptsInt(dflt = 30, min = 1, max = 999, step = 1)
	private int daysPerBatch;

	@ConfigurableField(editor = EditorSubclass.class,
			label = "Earliest Date Time", description = "Earliest date/time to fetch Observations for.")
	@EditorSubclass.EdOptsSubclass(iface = TimeGen.class)
	private TimeGen minTime;

	private SensorThingsService targetService;
	private FrostUtils frostUtils;

	@Override
	public void configure(JsonElement config, SensorThingsService context, Object edtCtx, ConfigEditor<?> configEditor) throws ConfigurationException {
		this.targetService = context;
		AnnotatedConfigurable.super.configure(config, context, edtCtx, configEditor);
		frostUtils = new FrostUtils(targetService);
	}

	@Override
	public Iterator<List<Observation>> iterator() {
		return new ObservationListIter(this);
	}

	public Thing findTargetFor(Thing sourceThing) throws ServiceFailureException {
		if (sourceThing == null) {
			return null;
		}
		final EntityList<Thing> thingList = addOrCreateFilter(targetService.things().query(), null, sourceThing.getName())
				.list();
		if (thingList.size() > 1) {
			throw new IllegalStateException("More than one thing found with name " + sourceThing.getName());
		}
		if (thingList.size() == 1) {
			return thingList.iterator().next();
		}
		return null;
	}

	public Datastream findTargetFor(Datastream sourceDs) throws ServiceFailureException {
		if (sourceDs == null) {
			return null;
		}
		final EntityList<Datastream> dsList = addOrCreateFilter(targetService.datastreams().query(), null, sourceDs.getName())
				.list();
		if (dsList.size() > 1) {
			throw new IllegalStateException("More than one thing found with name " + sourceDs.getName());
		}
		if (dsList.size() == 1) {
			return dsList.iterator().next();
		}
		return null;
	}

	private static class ObservationListIter implements Iterator<List<Observation>> {

		private final ImporterSta parent;
		private final SensorThingsService service;
		private final Iterator<Thing> sourceThings;
		private Thing currentSourceThing;
		private Thing currentTargetThing;
		private Iterator<Datastream> sourceDatastreams;
		private Datastream currentSourceDatastream;
		private Datastream currentTargetDatastream;
		private Instant startTime;
		private Instant finalTime;
		private Iterator<Observation> sourceObservations;

		public ObservationListIter(ImporterSta parent) {
			this.parent = parent;
			service = new SensorThingsService();
			Iterator<Thing> things = null;
			try {
				service.setEndpoint(new URL(parent.sourceServiceUrl));
				if (!Utils.isNullOrEmpty(parent.serviceUrlReplace)) {
					service.setUrlReplace(parent.serviceUrlReplace);
				}
				if (parent.sourceAuthMethod != null) {
					parent.sourceAuthMethod.setAuth(service);
				}
				things = service.things().query().orderBy("id").top(1000).list().fullIterator();
			} catch (MalformedURLException ex) {
				LOGGER.error("Failed to create service", ex);
			} catch (StatusCodeException ex) {
				LOGGER.error("Failed to fetch data: {} - {}\n{}", ex.getStatusCode(), ex.getStatusMessage(), ex.getReturnedContent());
			} catch (ServiceFailureException ex) {
				LOGGER.error("Failed to fetch data: {}", ex.getMessage());
			}
			sourceThings = things;
		}

		private void nextThing() throws ServiceFailureException {
			if (sourceThings.hasNext()) {
				currentSourceThing = sourceThings.next();
				currentTargetThing = parent.findTargetFor(currentSourceThing);
				LOGGER.debug("  {} -> {}", currentSourceThing, currentTargetThing);
			} else {
				currentSourceThing = null;
				currentTargetThing = null;
			}
		}

		private void nextDatastream() throws ServiceFailureException {
			if (sourceDatastreams == null || !sourceDatastreams.hasNext()) {
				nextThing();
				if (currentSourceThing == null || currentTargetThing == null) {
					currentSourceDatastream = null;
					currentTargetDatastream = null;
				}
				sourceDatastreams = currentSourceThing.datastreams()
						.query()
						.orderBy("id asc")
						.top(10000)
						.list()
						.fullIterator();
			}
			if (sourceDatastreams.hasNext()) {
				currentSourceDatastream = sourceDatastreams.next();
				currentTargetDatastream = parent.findTargetFor(currentSourceDatastream);
				startTime = parent.minTime.getInstant(currentSourceDatastream);
				final Interval phenomenonTime = currentSourceDatastream.getPhenomenonTime();
				if (phenomenonTime == null) {
					finalTime = Instant.now();
				} else {
					final Instant phenTimeEnd = phenomenonTime.getEnd();
					finalTime = phenTimeEnd;
				}
				LOGGER.debug("    {} -> {}", currentSourceDatastream, currentTargetDatastream);
			} else {
				currentSourceDatastream = null;
				currentTargetDatastream = null;
			}
		}

		private List<Observation> nextObservations() throws ServiceFailureException {
			if (currentTargetDatastream == null || sourceObservations == null || !sourceObservations.hasNext()) {
				if (startTime == null || startTime.isAfter(finalTime)) {
					nextDatastream();
				}
				if (currentSourceDatastream == null || currentTargetDatastream == null) {
					return Collections.emptyList();
				}
				Instant endTime = startTime.plus(parent.daysPerBatch, ChronoUnit.DAYS);
				sourceObservations = currentSourceDatastream.observations()
						.query()
						.orderBy("phenomenonTime asc")
						.filter("phenomenonTime ge " + startTime.toString() + " and phenomenonTime lt " + endTime)
						.top(10000)
						.list()
						.fullIterator();
				startTime = endTime;
			}
			List<Observation> result = new ArrayList<>(10000);
			while (sourceObservations.hasNext() && result.size() < 10000) {
				var sourceObs = sourceObservations.next();
				var targetObs = new Observation();
				targetObs.setDatastream(currentTargetDatastream);
				targetObs.setParameters(sourceObs.getParameters());
				targetObs.setPhenomenonTime(sourceObs.getPhenomenonTime());
				targetObs.setResult(sourceObs.getResult());
				targetObs.setResultQuality(sourceObs.getResultQuality());
				targetObs.setResultTime(sourceObs.getResultTime());
				targetObs.setValidTime(sourceObs.getValidTime());
				result.add(targetObs);
			}
			return result;
		}

		@Override
		public boolean hasNext() {
			return sourceThings != null && sourceThings.hasNext()
					|| sourceDatastreams != null && sourceDatastreams.hasNext()
					|| sourceObservations != null && sourceObservations.hasNext();
		}

		@Override
		public List<Observation> next() {
			try {
				return nextObservations();
			} catch (StatusCodeException ex) {
				LOGGER.error("Failed to fetch data: {} - {}\n{}", ex.getStatusCode(), ex.getStatusMessage(), ex.getReturnedContent());
			} catch (ServiceFailureException ex) {
				LOGGER.error("Failed to fetch data", ex);
			}
			return Collections.emptyList();
		}

	}

}
