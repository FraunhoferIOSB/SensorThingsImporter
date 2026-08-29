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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.validator;

import static de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing.EP_PARAMETERS;
import static de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing.EP_PHENOMENONTIME;

import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.frostclient.SensorThingsService;
import de.fraunhofer.iosb.ilt.frostclient.exception.ServiceFailureException;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.frostclient.model.ModelRegistry;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11MultiDatastream;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ObservationUploader;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates Observations by checking if one already exists with the same values
 * for a set of parameters.
 */
public class ValidatorByParameter implements Validator {

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ValidatorByParameter.class);

    @ConfigurableField(
            label = "parameters",
            description = "The parameters to check. Comma separated, no spaces.",
            editor = EditorString.class)
    @EditorString.EdOptsString(dflt = "importFileId,importFileBase")
    private String parameter;

    @ConfigurableField(
            label = "Check Time",
            description = "Check the phenomenonTime too",
            editor = EditorBoolean.class)
    @EditorBoolean.EdOptsBool()
    private boolean checkPhenomenonTime;

    @ConfigurableField(
            label = "Update",
            description = "Update existing observations.",
            editor = EditorBoolean.class)
    @EditorBoolean.EdOptsBool()
    private boolean update;

    private List<String> parameters;
    private SensorThingsV11Sensing mdl11;
    private SensorThingsV11MultiDatastream mdlMds;

    @Override
    public void init(ObservationUploader uploader) {
        String[] split = parameter.split(",");
        parameters = Arrays.asList(split);
        try {
            final SensorThingsService service = uploader.getService();
            final ModelRegistry mr = service.getModelRegistry();
            mdl11 = mr.getModel(SensorThingsV11Sensing.class);
            mdlMds = mr.getModel(SensorThingsV11MultiDatastream.class);
        } catch (MalformedURLException ex) {
            throw new ImportException(ex);
        }
    }

    private String buildFilter(Entity obs) {
        StringBuilder filter = new StringBuilder();
        boolean first = true;
        if (checkPhenomenonTime) {
            filter.append("phenomenonTime eq ");
            filter.append(obs.getProperty(EP_PHENOMENONTIME).toString());
            first = false;
        }
        for (String param : parameters) {
            if (first) {
                first = false;
            } else {
                filter.append(" and ");
            }
            Object paramValueRaw = obs.getProperty(EP_PARAMETERS).get(param);
            String paramUrlValue;
            if (paramValueRaw instanceof Number) {
                paramUrlValue = paramValueRaw.toString();
            } else {
                paramUrlValue = "'" + paramValueRaw + "'";
            }
            filter.append("Parameters/").append(param).append(" eq ").append(paramUrlValue);
        }
        return filter.toString();
    }

    @Override
    public boolean isValid(Entity obs) throws ImportException {
        String filter = buildFilter(obs);
        try {
            Entity ds = obs.getProperty(mdl11.npObservationDatastream);
            if (ds != null) {
                Entity first = ds.query(mdl11.npDatastreamObservations)
                        .select("@iot.id", "Parameters")
                        .filter(filter)
                        .first();
                if (first == null) {
                    return true;
                } else {
                    LOGGER.trace("Observation {} with given Parameters {} = {} exists.", first, parameters, obs.getProperty(EP_PARAMETERS));
                    if (update) {
                        obs.setPrimaryKeyValues(first.getPrimaryKeyValues());
                        return true;
                    }
                    return false;
                }
            }
            Entity mds = obs.getProperty(mdlMds.npObservationMultidatastream);
            if (mds != null) {
                Entity first = mds.query(mdlMds.npMultidatastreamObservations)
                        .select("@iot.id", "Parameters")
                        .filter(filter)
                        .first();
                if (first == null) {
                    return true;
                } else {
                    LOGGER.trace("Observation {} with given Parameter {} = {} exists.", first, parameters, obs.getProperty(EP_PARAMETERS));
                    if (update) {
                        obs.setPrimaryKeyValues(first.getPrimaryKeyValues());
                        return true;
                    }
                    return false;
                }
            }
            throw new ImportException("Observation has no Datastream of Multidatastream set!");
        } catch (ServiceFailureException ex) {
            throw new ImportException("Failed to validate.", ex);
        }
    }

    /**
     * @return the parameter
     */
    public String getParameter() {
        return parameter;
    }

    /**
     * @param parameter the parameter to set
     */
    public void setParameter(String parameter) {
        this.parameter = parameter;
    }

    /**
     * @return the update
     */
    public boolean isUpdate() {
        return update;
    }

    /**
     * @param update the update to set
     */
    public void setUpdate(boolean update) {
        this.update = update;
    }

}
