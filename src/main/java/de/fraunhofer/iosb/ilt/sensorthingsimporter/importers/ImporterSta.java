/*
 * Copyright (C) 2026 Fraunhofer Institut IOSB, Fraunhoferstr. 1, D 76131
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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.importers;

import static de.fraunhofer.iosb.ilt.frostclient.models.CommonProperties.EP_NAME;
import static de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing.EP_PARAMETERS;
import static de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing.EP_PHENOMENONTIME;
import static de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing.EP_PHENOMENONTIMEDS;
import static de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing.EP_RESULT;
import static de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing.EP_RESULTQUALITY;
import static de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing.EP_RESULTTIME;
import static de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing.EP_VALIDTIME;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.FrostUtils.addOrCreateFilter;

import de.fraunhofer.iosb.ilt.configurable.Utils;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.frostclient.SensorThingsService;
import de.fraunhofer.iosb.ilt.frostclient.auth.AuthMethod;
import de.fraunhofer.iosb.ilt.frostclient.exception.ServiceFailureException;
import de.fraunhofer.iosb.ilt.frostclient.exception.StatusCodeException;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.frostclient.model.EntitySet;
import de.fraunhofer.iosb.ilt.frostclient.model.ModelRegistry;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11MultiDatastream;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.TimeInterval;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.Importer;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.timegen.TimeGen;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.FrostUtils;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import net.time4j.Moment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Imports data from another STA service.
 */
public class ImporterSta implements Importer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ImporterSta.class.getName());

    @ConfigurableField(editor = EditorString.class,
            label = "Source Service URL", description = "The url of the server to import from.")
    @EditorString.EdOptsString(dflt = "http://localhost:8080/FROST-Server/v1.1")
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
    private SensorThingsV11Sensing mdl11;
    private SensorThingsV11MultiDatastream mdlMds;

    @Override
    public void init(SensorThingsService service) throws ImportException {
        this.targetService = service;
        final ModelRegistry mr = service.getModelRegistry();
        mdl11 = mr.getModel(SensorThingsV11Sensing.class);
        mdlMds = mr.getModel(SensorThingsV11MultiDatastream.class);
        frostUtils = new FrostUtils(targetService);
    }

    @Override
    public Iterator<List<Entity>> iterator() {
        return new ObservationListIter(this);
    }

    public Entity findTargetForThing(Entity sourceThing) throws ServiceFailureException {
        if (sourceThing == null) {
            return null;
        }
        final EntitySet thingList = addOrCreateFilter(targetService.query(mdl11.etThing), null, sourceThing.getProperty(EP_NAME))
                .list();
        if (thingList.size() > 1) {
            throw new IllegalStateException("More than one thing found with name " + sourceThing);
        }
        if (thingList.size() == 1) {
            return thingList.iterator().next();
        }
        return null;
    }

    public Entity findTargetForDs(Entity sourceDs) throws ServiceFailureException {
        if (sourceDs == null) {
            return null;
        }
        final EntitySet dsList = addOrCreateFilter(targetService.query(mdl11.etDatastream), null, sourceDs.getProperty(EP_NAME))
                .list();
        if (dsList.size() > 1) {
            throw new IllegalStateException("More than one datastream found with name " + sourceDs.getProperty(EP_NAME));
        }
        if (dsList.size() == 1) {
            return dsList.iterator().next();
        }
        return null;
    }

    private static class ObservationListIter implements Iterator<List<Entity>> {

        private final ImporterSta parent;
        private final SensorThingsService service;
        private final Iterator<Entity> sourceThings;
        private Entity currentSourceThing;
        private Entity currentTargetThing;
        private Iterator<Entity> sourceDatastreams;
        private Entity currentSourceDatastream;
        private Entity currentTargetDatastream;
        private Moment startTime;
        private Moment finalTime;
        private Iterator<Entity> sourceObservations;

        public ObservationListIter(ImporterSta parent) {
            this.parent = parent;
            service = new SensorThingsService();
            Iterator<Entity> things = null;
            try {
                service.setBaseUrl(new URL(parent.sourceServiceUrl));
                if (!Utils.isNullOrEmpty(parent.serviceUrlReplace)) {
                    service.setUrlReplace(parent.serviceUrlReplace);
                }
                if (parent.sourceAuthMethod != null) {
                    parent.sourceAuthMethod.setAuth(service);
                }
                service.init();
                things = service.query(parent.mdl11.etThing).orderBy("id").top(1000).list().iterator();
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
                currentTargetThing = parent.findTargetForThing(currentSourceThing);
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
                sourceDatastreams = currentSourceThing.dao(parent.mdl11.npThingDatastreams)
                        .query()
                        .orderBy("id asc")
                        .top(10000)
                        .list()
                        .iterator();
            }
            if (sourceDatastreams.hasNext()) {
                currentSourceDatastream = sourceDatastreams.next();
                currentTargetDatastream = parent.findTargetForDs(currentSourceDatastream);
                startTime = parent.minTime.getMomentFromDs(currentSourceDatastream);
                TimeInterval phenomenonTime = currentSourceDatastream.getProperty(EP_PHENOMENONTIMEDS);
                if (phenomenonTime == null) {
                    finalTime = Moment.nowInSystemTime();
                } else {
                    Moment phenTimeEnd = phenomenonTime.getEnd();
                    finalTime = phenTimeEnd;
                }
                LOGGER.debug("    {} -> {}", currentSourceDatastream, currentTargetDatastream);
            } else {
                currentSourceDatastream = null;
                currentTargetDatastream = null;
            }
        }

        private List<Entity> nextObservations() throws ServiceFailureException {
            if (currentTargetDatastream == null || sourceObservations == null || !sourceObservations.hasNext()) {
                if (startTime == null || startTime.isAfter(finalTime)) {
                    nextDatastream();
                }
                if (currentSourceDatastream == null || currentTargetDatastream == null) {
                    return Collections.emptyList();
                }
                Moment endTime = startTime.plus(parent.daysPerBatch, TimeUnit.DAYS);
                sourceObservations = currentSourceDatastream.dao(parent.mdl11.npDatastreamObservations)
                        .query()
                        .orderBy("phenomenonTime asc")
                        .filter("phenomenonTime ge " + startTime.toString() + " and phenomenonTime lt " + endTime)
                        .top(10000)
                        .list()
                        .iterator();
                startTime = endTime;
            }
            List<Entity> result = new ArrayList<>(10000);
            while (sourceObservations.hasNext() && result.size() < 10000) {
                var sourceObs = sourceObservations.next();
                var targetObs = parent.mdl11.newObservation(sourceObs.getProperty(EP_RESULT), currentTargetDatastream);
                FrostUtils.copyProperty(sourceObs, targetObs, EP_PARAMETERS);
                FrostUtils.copyProperty(sourceObs, targetObs, EP_PHENOMENONTIME);
                FrostUtils.copyProperty(sourceObs, targetObs, EP_RESULTQUALITY);
                FrostUtils.copyProperty(sourceObs, targetObs, EP_RESULTTIME);
                FrostUtils.copyProperty(sourceObs, targetObs, EP_VALIDTIME);
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
        public List<Entity> next() {
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
