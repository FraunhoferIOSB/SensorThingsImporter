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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.utils;

import de.fraunhofer.iosb.ilt.frostclient.SensorThingsService;
import de.fraunhofer.iosb.ilt.frostclient.exception.ServiceFailureException;
import de.fraunhofer.iosb.ilt.frostclient.json.SimpleJsonMapper;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.frostclient.model.EntitySet;
import de.fraunhofer.iosb.ilt.frostclient.model.ModelRegistry;
import de.fraunhofer.iosb.ilt.frostclient.model.property.EntityPropertyMain;
import de.fraunhofer.iosb.ilt.frostclient.model.property.type.TypeComplex;
import de.fraunhofer.iosb.ilt.frostclient.models.CommonProperties;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11MultiDatastream;
import de.fraunhofer.iosb.ilt.frostclient.models.SensorThingsV11Sensing;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.MapValue;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.TimeInterval;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.TimeObject;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.TimeValue;
import de.fraunhofer.iosb.ilt.frostclient.models.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.frostclient.query.Query;
import de.fraunhofer.iosb.ilt.frostclient.utils.StringHelper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import net.time4j.Moment;
import org.apache.commons.lang3.StringUtils;
import org.geojson.GeoJsonObject;
import org.geojson.Point;
import org.geotools.api.geometry.MismatchedDimensionException;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.geometry.Position2D;
import org.geotools.referencing.CRS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * Utilities for accessing FROST.
 */
public final class FrostUtils {

    public static final ZoneId ZONE_ID_Z = ZoneId.of("Z");

    /**
     * The NULL unit to use for "empty" units.
     */
    public static final UnitOfMeasurement NULL_UNIT = new UnitOfMeasurement(null, null, null);

    /**
     * The encoding type for GeoJSON.
     */
    public static final String ENCODING_GEOJSON = "application/geo+json";

    /**
     * The content type for GeoJSON.
     */
    public static final String CONTENT_TYPE_GEOJSON = ENCODING_GEOJSON;

    public static final String OBS_TYPE_MEASUREMENT = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement";

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(FrostUtils.class);

    private final SensorThingsService service;
    private SensorThingsV11Sensing mdl11;
    private SensorThingsV11MultiDatastream mdlMds;

    private boolean dryRun;

    private int countInsert;
    private int countUpdate;

    public FrostUtils(final SensorThingsService service) {
        this.service = service;
        final ModelRegistry mr = service.getModelRegistry();
        mdl11 = mr.getModel(SensorThingsV11Sensing.class);
        mdlMds = mr.getModel(SensorThingsV11MultiDatastream.class);
    }

    public void setDryRun(final boolean dryRun) {
        this.dryRun = dryRun;
    }

    public SensorThingsService getService() {
        return service;
    }

    public static String camelCase(final String name) {
        final String[] parts = StringUtils.split(name, '_');
        final StringBuilder result = new StringBuilder(parts[0].toLowerCase());
        for (int idx = 1; idx < parts.length; idx++) {
            final String part = parts[idx];
            result.append(part.substring(0, 1).toUpperCase());
            result.append(part.substring(1).toLowerCase());
        }
        return result.toString();
    }

    public void update(final Entity entity) throws ServiceFailureException {
        if (dryRun) {
            LOGGER.info("Dry Run: Not updating entity {}", entity);
        } else {
            service.update(entity);
            countUpdate++;
        }
    }

    public void create(final Entity entity) throws ServiceFailureException {
        if (dryRun) {
            LOGGER.info("Dry Run: Not creating entity {}", entity);
        } else {
            service.create(entity);
            countInsert++;
        }
    }

    public int getCountInsert() {
        return countInsert;
    }

    public int getCountUpdate() {
        return countUpdate;
    }

    public void resetCounts() {
        countInsert = 0;
        countUpdate = 0;
    }

    public void delete(List<Entity> entities, int threads) throws ServiceFailureException {
        if (threads <= 1) {
            for (Entity entity : entities) {
                service.delete(entity);
            }
            return;
        }
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>();
        for (Entity entity : entities) {
            futures.add(executor.submit(() -> {
                try {
                    service.delete(entity);
                } catch (ServiceFailureException ex) {
                    LOGGER.error("Failed to delete {}", entity, ex);
                }
            }));
        }
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException ex) {
                LOGGER.error("Maybe Failed to delete?", ex);
            }
        }
        executor.shutdownNow();
    }

    public Entity findOrCreateThing(final String filter, final String name, final String description, final Map<String, Object> properties, final Entity location, final Entity cachedThing) throws ServiceFailureException {
        final Entity thing = mdl11.newThing(name, description, properties);
        if (location != null) {
            thing.addNavigationEntity(mdl11.npThingLocations, location.withOnlyPk());
        }
        return findOrCreateThing(filter, thing, cachedThing);
    }

    public Entity findOrCreateThing(final String filter, final Entity newThing, final Entity cachedThing) throws ServiceFailureException {
        Entity thing = null;
        if (cachedThing != null) {
            thing = cachedThing;
        } else {
            final EntitySet thingList = addOrCreateFilter(service.query(mdl11.etThing), filter, newThing.getProperty(CommonProperties.EP_NAME))
                    .expand("Locations($select=id)")
                    .list();
            if (thingList.size() > 1) {
                throw new IllegalStateException("More than one thing found with filter " + filter);
            }
            if (thingList.size() == 1) {
                thing = thingList.iterator().next();
            }
        }
        if (thing == null) {
            LOGGER.info("Creating Thing {}.", newThing);
            thing = newThing;
            create(thing);
        } else {
            maybeUpdateThing(newThing, thing);
        }
        return thing;
    }

    /**
     * Updates property of target if its value is different from the value in
     * source.
     *
     * @param <P> The type of property
     * @param source the source entity to maybe copy from.
     * @param target the target entity to maybe copy to.
     * @param property the property to maybe copy from source to target.
     * @return true if property was copied from source to target, false if no
     * copy happened.
     */
    public static <P> boolean compareAndUpdate(Entity source, Entity target, EntityPropertyMain<P> property) {
        P sourceP = source.getProperty(property);
        P targetP = target.getProperty(property);
        if (Objects.equals(sourceP, targetP)) {
            return false;
        }
        target.setProperty(property, sourceP);
        return true;
    }

    public static <P> void copyProperty(Entity source, Entity target, EntityPropertyMain<P> property) {
        target.setProperty(property, target.getProperty(property));
    }

    public boolean maybeUpdateThing(final Entity newThing, final Entity thingToUpdate) throws ServiceFailureException {
        boolean updated = false;
        boolean updatedLocation = false;
        updated = compareAndUpdate(newThing, thingToUpdate, CommonProperties.EP_NAME) || updated;
        updated = compareAndUpdate(newThing, thingToUpdate, CommonProperties.EP_DESCRIPTION) || updated;
        updated = addProperties(thingToUpdate, newThing, CommonProperties.EP_PROPERTIES, 5) || updated;

        EntitySet newLocations = newThing.getProperty(mdl11.npThingLocations, false);
        if (newLocations != null && !newLocations.isEmpty()) {
            final Entity newLocation = newLocations.toList().get(0);
            final List<Entity> locationListToUpdate = thingToUpdate.getProperty(mdl11.npThingLocations).toList();
            if (!newLocation.primaryKeyFullySet()) {
                // "new" Location in newThing.
                if (locationListToUpdate.isEmpty()) {
                    final Entity created = findOrCreateLocation(null, newLocation, null);
                    thingToUpdate.addNavigationEntity(mdl11.npThingLocations, created.withOnlyPk());
                    updated = true;
                    updatedLocation = true;
                } else if (locationListToUpdate.size() == 1) {
                    final Entity oldLocation = service.dao(mdl11.etLocation).find(locationListToUpdate.get(0).getPrimaryKeyValues());
                    maybeUpdateLocation(newLocation, oldLocation);
                } else {
                    LOGGER.error("Can't check locations for Things with multiple locations if updated Location has no ID.");
                }
            } else {
                if (locationListToUpdate.isEmpty()) {
                    thingToUpdate.addNavigationEntity(mdl11.npThingLocations, newLocation.withOnlyPk());
                    updated = true;
                    updatedLocation = true;
                } else {
                    final boolean found = locationListToUpdate.stream().anyMatch(loc -> loc.getPrimaryKeyValues().equals(newLocation.getPrimaryKeyValues()));
                    if (!found) {
                        thingToUpdate.unsetProperty(mdl11.npThingLocations);
                        thingToUpdate.addNavigationEntity(mdl11.npThingLocations, newLocation.withOnlyPk());
                        updated = true;
                        updatedLocation = true;
                    }
                }
            }
        }
        if (updated) {
            if (!updatedLocation) {
                final List<Entity> thingLocations = thingToUpdate.getProperty(mdl11.npThingLocations).toList();
                thingToUpdate.unsetProperty(mdl11.npThingLocations);
                update(thingToUpdate);
                thingToUpdate.addNavigationEntity(mdl11.npThingLocations, thingLocations);
            } else {
                update(thingToUpdate);
            }
        }
        return updated;
    }

    public Entity findOrCreateSensor(final String filter, final String name, final String description, final String encodingType, final Object metadata, final Map<String, Object> properties, final Entity cached) throws ServiceFailureException {
        final Entity sensor = mdl11.newSensor(name, description, encodingType, metadata)
                .setProperty(CommonProperties.EP_PROPERTIES, new MapValue(TypeComplex.STA_MAP, properties));
        return findOrCreateSensor(filter, sensor, cached);
    }

    public Entity findOrCreateSensor(final String filter, final Entity newSensor, final Entity cachedSensor) throws ServiceFailureException {
        Entity sensor = null;
        if (cachedSensor != null) {
            sensor = cachedSensor;
        } else {
            final Query query = service.query(mdl11.etSensor);
            final EntitySet sensorList = addOrCreateFilter(query, filter, newSensor.getProperty(CommonProperties.EP_NAME)).list();
            if (sensorList.size() > 1) {
                throw new IllegalStateException("More than one sensor with name " + newSensor);
            }
            if (sensorList.size() == 1) {
                sensor = sensorList.iterator().next();
            }
        }
        if (sensor == null) {
            LOGGER.info("Creating Sensor {}.", newSensor);
            sensor = newSensor;
            create(sensor);
        } else {
            mayeUpdateSensor(newSensor, sensor);
        }
        return sensor;
    }

    public boolean mayeUpdateSensor(final Entity newSensor, final Entity cached) throws ServiceFailureException {
        boolean update = false;
        update = compareAndUpdate(newSensor, cached, CommonProperties.EP_NAME) || update;
        update = compareAndUpdate(newSensor, cached, CommonProperties.EP_DESCRIPTION) || update;
        update = compareAndUpdate(newSensor, cached, CommonProperties.EP_ENCODINGTYPE) || update;
        update = compareAndUpdate(newSensor, cached, SensorThingsV11Sensing.EP_METADATA) || update;
        update = addProperties(cached, newSensor, CommonProperties.EP_PROPERTIES, 5) || update;
        if (update) {
            update(cached);
        }
        return update;
    }

    public Entity findOrCreateFeature(final String filter, final String name, final String description, final GeoJsonObject geoJson, final Map<String, Object> properties, final Entity cached) throws ServiceFailureException {
        final Entity foi = mdl11.newFeatureOfInterest(name, description, CONTENT_TYPE_GEOJSON, geoJson)
                .setProperty(CommonProperties.EP_PROPERTIES, new MapValue(TypeComplex.STA_MAP, properties));
        return findOrCreateFeature(filter, foi, cached);
    }

    public Entity findOrCreateFeature(final String filter, final Entity newFeature, final Entity cachedFeature) throws ServiceFailureException {
        Entity foi = null;
        if (cachedFeature != null) {
            foi = cachedFeature;
        } else {
            final Query query = service.query(mdl11.etFeatureOfInterest);
            final EntitySet foiList = addOrCreateFilter(query, filter, newFeature.getProperty(CommonProperties.EP_NAME)).list();
            if (foiList.size() > 1) {
                throw new IllegalStateException("More than one FeatureOfInterest with name " + newFeature);
            }
            if (foiList.size() == 1) {
                foi = foiList.iterator().next();
            }
        }
        if (foi == null) {
            LOGGER.info("Creating Feature {}.", newFeature);
            foi = newFeature;
            create(foi);
        } else {
            maybeUpdateFeatureOfInterest(newFeature, foi);
        }
        return foi;
    }

    public boolean maybeUpdateFeatureOfInterest(final Entity newFeature, final Entity cached) throws ServiceFailureException {
        boolean update = false;
        update = compareAndUpdate(newFeature, cached, CommonProperties.EP_NAME) || update;
        update = compareAndUpdate(newFeature, cached, CommonProperties.EP_DESCRIPTION) || update;
        update = addProperties(cached, newFeature, CommonProperties.EP_PROPERTIES, 5) || update;

        final ObjectMapper objectMapper = SimpleJsonMapper.getSimpleObjectMapper();
        try {
            final Object newFoiFeature = newFeature.getProperty(SensorThingsV11Sensing.EP_FEATURE);
            final Object cachedFoiFeature = cached.getProperty(SensorThingsV11Sensing.EP_FEATURE);
            if (!objectMapper.writeValueAsString(newFoiFeature).equals(objectMapper.writeValueAsString(cachedFoiFeature))) {
                update = true;
                LOGGER.debug("Location changed from {} to {}", cachedFoiFeature, newFoiFeature);
                cached.setProperty(SensorThingsV11Sensing.EP_FEATURE, newFoiFeature);
            }
        } catch (final JacksonException exc) {
            LOGGER.error("Failed to compare geoJson objects.", exc);
        }

        if (update) {
            update(cached);
        }
        return update;
    }

    public Entity findOrCreateOp(final String filter, final String name, final String def, final String description, final Map<String, Object> properties, final Entity cached) throws ServiceFailureException {
        final Entity observedProperty = mdl11.newObservedProperty(name, def, description)
                .setProperty(CommonProperties.EP_PROPERTIES, new MapValue(TypeComplex.STA_MAP, properties));
        return findOrCreateOp(filter, observedProperty, cached);
    }

    public Entity findOrCreateOp(final String filter, final Entity newObsProp, final Entity cachedObsProp) throws ServiceFailureException {
        Entity observedProperty = null;
        if (cachedObsProp != null) {
            observedProperty = cachedObsProp;
        } else {
            final Query query = service.query(mdl11.etObservedProperty);
            final EntitySet opList = addOrCreateFilter(query, filter, newObsProp.getProperty(CommonProperties.EP_NAME)).list();
            if (opList.size() > 1) {
                throw new IllegalStateException("More than one observedProperty with name " + newObsProp);
            }
            if (opList.size() == 1) {
                observedProperty = opList.iterator().next();
            }
        }
        if (observedProperty == null) {
            LOGGER.info("Creating ObservedProperty {}.", newObsProp);
            observedProperty = newObsProp;
            create(observedProperty);
        } else {
            maybeUpdateOp(newObsProp, observedProperty);
        }
        return observedProperty;
    }

    public boolean maybeUpdateOp(final Entity newObsProp, final Entity cached) throws ServiceFailureException {
        boolean update = false;
        update = compareAndUpdate(newObsProp, cached, CommonProperties.EP_NAME) || update;
        update = compareAndUpdate(newObsProp, cached, CommonProperties.EP_DESCRIPTION) || update;
        update = addProperties(cached, newObsProp, CommonProperties.EP_PROPERTIES, 5) || update;
        if (update) {
            update(cached);
        }
        return update;
    }

    public Entity findOrCreateDatastream(
            final String filter,
            final String name,
            final String desc,
            final Map<String, Object> properties,
            final UnitOfMeasurement uom,
            final Entity t,
            final Entity op,
            final Entity s,
            final Entity cached) throws ServiceFailureException {
        Entity ds = mdl11.newDatastream(name, desc, OBS_TYPE_MEASUREMENT, uom)
                .setProperty(CommonProperties.EP_PROPERTIES, new MapValue(TypeComplex.STA_MAP, properties))
                .setProperty(mdl11.npDatastreamThing, t)
                .setProperty(mdl11.npDatastreamSensor, s)
                .setProperty(mdl11.npDatastreamObservedproperty, op);
        return findOrCreateDatastream(filter, ds, cached);
    }

    public Entity findOrCreateDatastream(final String filter, final Entity newDatastream, final Entity cached) throws ServiceFailureException {
        Entity datastream = null;
        if (cached != null) {
            datastream = cached;
        } else {
            final Query query = newDatastream.getProperty(mdl11.npDatastreamThing).query(mdl11.npThingDatastreams);
            final EntitySet datastreamList = addOrCreateFilter(query, filter, newDatastream.getProperty(CommonProperties.EP_NAME)).list();
            if (datastreamList.size() > 1) {
                throw new IllegalStateException("More than one datastream matches filter " + filter);
            }
            if (datastreamList.size() == 1) {
                datastream = datastreamList.iterator().next();
            }
        }
        if (datastream == null) {
            LOGGER.info("Creating Datastream {}.", newDatastream);
            datastream = newDatastream;
            create(datastream);
        } else {
            maybeUpdateDatastream(newDatastream, datastream);
        }
        return datastream;
    }

    public boolean maybeUpdateDatastream(final Entity newDatastream, final Entity cached) throws ServiceFailureException {
        boolean update = false;
        update = compareAndUpdate(newDatastream, cached, CommonProperties.EP_NAME) || update;
        update = compareAndUpdate(newDatastream, cached, CommonProperties.EP_DESCRIPTION) || update;
        update = addProperties(cached, newDatastream, CommonProperties.EP_PROPERTIES, 5) || update;
        update = compareAndUpdate(newDatastream, cached, SensorThingsV11Sensing.EP_UNITOFMEASUREMENT) || update;

        final Entity newObsProp = newDatastream.getProperty(mdl11.npDatastreamObservedproperty);
        final Entity cachedObsProp = cached.getProperty(mdl11.npDatastreamObservedproperty);
        if (!Objects.equals(cachedObsProp.getPrimaryKeyValues(), newObsProp.getPrimaryKeyValues())) {
            cached.setProperty(mdl11.npDatastreamObservedproperty, newObsProp.withOnlyPk());
            update = true;
        }
        if (update) {
            update(cached);
        }
        return update;
    }

    public Entity findOrCreateMultiDatastream(
            final String filter, final String name, final String desc,
            final List<UnitOfMeasurement> uoms, final Entity thing,
            final List<Entity> observedProperties, final Entity sensor, final Map<String, Object> props,
            final Entity cached) throws ServiceFailureException {
        Entity mds = null;
        if (cached != null) {
            mds = cached;
        } else {
            final Query query = service.query(mdlMds.etMultiDatastream);
            final EntitySet mdsList = addOrCreateFilter(query, filter, name).list();
            if (mdsList.size() > 1) {
                throw new IllegalStateException("More than one multidatastream with name " + name);
            }

            if (mdsList.size() == 1) {
                mds = mdsList.iterator().next();
            }
        }
        if (mds == null) {
            LOGGER.info("Creating multiDatastream {}.", name);
            mds = mdlMds.newMultiDatastream(name, desc, uoms)
                    .setProperty(CommonProperties.EP_PROPERTIES, new MapValue(TypeComplex.STA_MAP, props))
                    .setProperty(mdlMds.npMultidatastreamThing, thing)
                    .setProperty(mdlMds.npMultidatastreamSensor, sensor)
                    .addNavigationEntity(mdlMds.npMultidatastreamObservedproperties, observedProperties);
            create(mds);
        } else {
            maybeUpdateMultiDatastream(name, desc, props, mds);
        }
        return mds;
    }

    public boolean maybeUpdateMultiDatastream(final String name, final String desc, final Map<String, Object> props, final Entity mdsToUpdate) throws ServiceFailureException {
        boolean update = false;
        if (!name.equals(mdsToUpdate.getProperty(CommonProperties.EP_NAME))) {
            update = true;
            mdsToUpdate.setProperty(CommonProperties.EP_NAME, name);
        }
        if (!desc.equals(mdsToUpdate.getProperty(CommonProperties.EP_DESCRIPTION))) {
            update = true;
            mdsToUpdate.setProperty(CommonProperties.EP_DESCRIPTION, desc);
        }
        if (mdsToUpdate.getProperty(CommonProperties.EP_PROPERTIES) == null && props != null) {
            mdsToUpdate.setProperty(CommonProperties.EP_PROPERTIES, new MapValue(TypeComplex.STA_MAP, props));
            update = true;
        }
        if (addProperties(mdsToUpdate.getProperty(CommonProperties.EP_PROPERTIES).getContent(), props, 5)) {
            update = true;
        }
        if (update) {
            update(mdsToUpdate);
        }
        return update;
    }

    public Entity findOrCreateLocation(final String name, final String description, final Map<String, Object> properties, final GeoJsonObject geoJson) throws ServiceFailureException {
        final String filter = "name eq '" + StringHelper.escapeForStringConstant(name) + "'";
        return findOrCreateLocation(filter, name, description, properties, geoJson, null);
    }

    public Entity findOrCreateLocation(final String filter, final String name, final String description, final Map<String, Object> properties, final GeoJsonObject geoJson, final Entity cached) throws ServiceFailureException {
        final Entity location = mdl11.newLocation(name, description, ENCODING_GEOJSON, geoJson)
                .setProperty(CommonProperties.EP_PROPERTIES, new MapValue(TypeComplex.STA_MAP, properties));
        return findOrCreateLocation(filter, location, cached);
    }

    public Entity findOrCreateLocation(final String filter, final Entity newLocation, final Entity cached) throws ServiceFailureException {
        Entity location = null;
        if (cached != null) {
            location = cached;
        } else {
            final Query query = service.query(mdl11.etLocation);
            final EntitySet lList = addOrCreateFilter(query, filter, newLocation.getProperty(CommonProperties.EP_NAME)).list();
            if (lList.size() > 1) {
                throw new IllegalStateException("More than one Location matches filter: " + filter);
            }
            if (lList.size() == 1) {
                location = lList.iterator().next();
            }
        }
        if (location == null) {
            LOGGER.info("Creating Location {}.", newLocation);
            location = newLocation;
            create(location);
        } else {
            maybeUpdateLocation(newLocation, location);
        }
        return location;
    }

    public boolean maybeUpdateLocation(final Entity newLocation, final Entity cached) throws ServiceFailureException {
        boolean update = false;
        update = compareAndUpdate(newLocation, cached, CommonProperties.EP_NAME) || update;
        update = compareAndUpdate(newLocation, cached, CommonProperties.EP_DESCRIPTION) || update;
        update = addProperties(cached, newLocation, CommonProperties.EP_PROPERTIES, 5) || update;

        final ObjectMapper objectMapper = SimpleJsonMapper.getSimpleObjectMapper();
        try {
            final Object newLocLocation = newLocation.getProperty(SensorThingsV11Sensing.EP_LOCATION);
            final Object cachedLocLocation = cached.getProperty(SensorThingsV11Sensing.EP_LOCATION);
            if (!objectMapper.writeValueAsString(newLocLocation).equals(objectMapper.writeValueAsString(cachedLocLocation))) {
                update = true;
                LOGGER.debug("Location changed from {} to {}", cachedLocLocation, newLocLocation);
                cached.setProperty(SensorThingsV11Sensing.EP_LOCATION, newLocLocation);
            }
        } catch (final JacksonException exc) {
            LOGGER.error("Failed to compare geoJson objects.", exc);
        }
        if (update) {
            update(cached);
        }
        return update;
    }

    public static String quoteForUrl(final Object in) {
        if (in instanceof Number) {
            return in.toString();
        }
        return "'" + StringHelper.escapeForStringConstant(String.valueOf(in)) + "'";
    }

    public static Query addOrCreateFilter(final Query query, final String filter, final String name) {
        if (StringHelper.isNullOrEmpty(filter)) {
            return query.filter("name eq '" + StringHelper.escapeForStringConstant(name) + "'");
        }
        return query.filter(filter);
    }

    public static Moment phenTimeToInstant(final TimeValue phenTime) {
        if (phenTime.isInterval()) {
            TimeInterval interval = phenTime.getInterval();
            return interval.getStart().plus(interval.getInterval().getSimpleDuration().dividedBy(2, RoundingMode.HALF_EVEN));
        }
        return phenTime.getInstant().getDateTime();
    }

    /**
     * Checks if all entries in source exist in target, with the same value.If
     * not, target is updated and true is returned. Sub-maps are recursed.
     *
     * @param target the target map to update
     * @param source the source map to get values from
     * @param property the property to merge.
     * @param maxDepth The maximum depth to recurse.
     * @return true if target was updated, false if not.
     */
    public static boolean addProperties(final Entity target, final Entity source, final EntityPropertyMain<MapValue> property, final int maxDepth) {
        MapValue sourceP = source.getProperty(property);
        MapValue targetP = target.getProperty(property);
        if (sourceP == null || sourceP.isEmpty()) {
            return false;
        }
        if (targetP == null) {
            target.setProperty(property, sourceP);
            return true;
        }
        return addProperties(targetP.getContent(), sourceP.getContent(), maxDepth);
    }

    /**
     * Checks if all entries in source exist in target, with the same value.If
     * not, target is updated and true is returned. Sub-maps are recursed.
     *
     * @param target the target map to update
     * @param source the source map to get values from
     * @param maxDepth The maximum depth to recurse.
     * @return true if target was updated, false if not.
     */
    public static boolean addProperties(final Map<String, Object> target, final Map<String, Object> source, final int maxDepth) {
        if (target == null) {
            return false;
        }

        boolean updated = false;
        for (final Map.Entry<String, Object> entry : source.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();
            if ((value == null || String.valueOf(value).isEmpty()) && !target.containsKey(key)) {
                continue;
            }
            if (!target.containsKey(key)) {
                target.put(key, value);
                updated = true;
            } else {
                final Object tValue = target.get(key);
                if (value instanceof Map) {
                    if (maxDepth > 0) {
                        final Map valueMap = (Map) value;
                        if (tValue instanceof Map) {
                            final Map tValueMap = (Map) tValue;
                            updated = updated || addProperties(tValueMap, valueMap, maxDepth - 1);
                        } else {
                            target.put(key, value);
                            updated = true;
                        }
                    }
                } else if (!resultCompare(value, tValue)) {
                    target.put(key, value);
                    updated = true;
                }

            }
        }
        return updated;
    }

    private static boolean resultCompare(final Object one, final Object two) {
        if (one == null) {
            return two == null;
        }
        if (two == null) {
            return false;
        }
        if (one.equals(two)) {
            return true;
        }

        try {
            if (one instanceof Long && two instanceof Integer) {
                return ((Long) one).equals(Long.valueOf((Integer) two));
            }
            if (two instanceof Long && one instanceof Integer) {
                return ((Long) two).equals(Long.valueOf((Integer) one));
            }
            if (one instanceof BigDecimal) {
                return ((BigDecimal) one).compareTo(new BigDecimal(two.toString())) == 0;
            }
            if (two instanceof BigDecimal) {
                return ((BigDecimal) two).compareTo(new BigDecimal(one.toString())) == 0;
            }
            if (one instanceof BigInteger) {
                return ((BigInteger) one).equals(new BigInteger(two.toString()));
            }
            if (two instanceof BigInteger) {
                return ((BigInteger) two).equals(new BigInteger(one.toString()));
            }
            if (one instanceof Instant) {
                return ((Instant) one).equals(Instant.parse(two.toString()));
            }
            if (two instanceof Instant) {
                return ((Instant) two).equals(Instant.parse(one.toString()));
            }
            if (one instanceof Collection cOne && two instanceof Collection cTwo) {
                final Iterator iTwo = cTwo.iterator();
                for (final Object itemOne : cOne) {
                    if (!iTwo.hasNext() || !resultCompare(itemOne, iTwo.next())) {
                        // Collection one is longer than two
                        return false;
                    }
                }
                if (iTwo.hasNext()) {
                    // Collection two is longer than one.
                    return false;
                }
                return true;
            }
        } catch (final NumberFormatException exc) {
            LOGGER.trace("Not both bigdecimal.", exc);
            // not both bigDecimal.
        }
        return false;
    }

    public static Moment instantFrom(TimeValue time) {
        return time.isInterval() ? time.getInterval().getStart() : time.getInstant().getDateTime();
    }

    /**
     * Creates an Instant from a timestamp. If the timestamp has no timezone
     * information, then the given timeZone is used.
     *
     * @param timestamp
     * @param timeZone
     * @return
     */
    public static Instant timestampToInstant(final Timestamp timestamp, final ZoneId timeZone) {
        try {
            return timestamp.toInstant();
        } catch (final Exception exc) {
            LOGGER.trace("Timestamp without timezone?", exc);
        }
        return ZonedDateTime.of(timestamp.toLocalDateTime(), timeZone).toInstant();

    }

    public static TimeValue timeObjectFrom(final Timestamp timestamp, final ZoneId timeZone) {
        try {
            final Instant instant = timestamp.toInstant();
            return TimeValue.create(instant);
        } catch (final Exception exc) {
            LOGGER.trace("Timestamp without timezone?", exc);
            return TimeValue.create(ZonedDateTime.of(timestamp.toLocalDateTime(), timeZone));
        }
    }

    public static TimeObject timeObjectFrom(final Date date) {
        final Instant instant = date.toInstant();
        return TimeValue.create(ZonedDateTime.from(instant));
    }

    /**
     * Creates a timeObject from timestamps. If the timestamps have no timezone
     * information, then the given timeZone is used.
     *
     * @param start the starting timeStamp
     * @param end the ending timeStamp
     * @param timeZone the time zone to cast the times to.
     * @return a timeobject.
     */
    public static TimeValue timeObjectFrom(final Timestamp start, final Timestamp end, final ZoneId timeZone) {
        final Instant instantStart = timestampToInstant(start, timeZone);
        final Instant instantEnd = timestampToInstant(end, timeZone);
        return TimeValue.create(instantStart, instantEnd);
    }

    /**
     * Creates a timeObject from timestamps. If the timestamps have no timezone
     * information, then the given timeZone is used.
     *
     * @param start the starting timeStamp
     * @param end the ending timeStamp
     * @return a timeobject.
     */
    public static TimeValue timeObjectFrom(final Date start, final Date end) {
        final Instant instantStart = start.toInstant();
        final Instant instantEnd = end.toInstant();
        return TimeValue.create(instantStart, instantEnd);
    }

    /**
     * Creates a timeObject from ISO timestamps.
     *
     * @param start the starting timeStamp
     * @param end the ending timeStamp
     * @return a timeobject.
     */
    public static TimeValue timeObjectFrom(final String start, final String end) {
        final Instant instantStart = ZonedDateTime.parse(start).toInstant();
        final Instant instantEnd = ZonedDateTime.parse(end).toInstant();
        return TimeValue.create(instantStart, instantEnd);
    }

    public static Point convertCoordinates(final double first, final double second, final String crsName, int numberScale) {
        if (StringHelper.isNullOrEmpty(crsName)) {
            return new Point(
                    new BigDecimal(second).setScale(numberScale, RoundingMode.HALF_EVEN).doubleValue(),
                    new BigDecimal(first).setScale(numberScale, RoundingMode.HALF_EVEN).doubleValue());
        }
        try {
            String fullCrs = crsName;
            if (!fullCrs.contains(":")) {
                fullCrs = "EPSG:" + fullCrs;
            }
            final CoordinateReferenceSystem sourceCrs = CRS.decode(fullCrs);
            final CoordinateReferenceSystem targetCrs = CRS.decode("EPSG:4326");
            final MathTransform transform = CRS.findMathTransform(sourceCrs, targetCrs);
            final Position2D sourcePoint = new Position2D(sourceCrs, first, second);
            final Position2D targetPoint = new Position2D(targetCrs);
            transform.transform(sourcePoint, targetPoint);
            return new Point(
                    new BigDecimal(targetPoint.y).setScale(numberScale, RoundingMode.HALF_EVEN).doubleValue(),
                    new BigDecimal(targetPoint.x).setScale(numberScale, RoundingMode.HALF_EVEN).doubleValue());
        } catch (MismatchedDimensionException | FactoryException | TransformException exc) {
            throw new RuntimeException("Failed to convert coordinates", exc);
        }
    }

    public static Point convertCoordinates(final String locationPos, final String locationSrsName, int numberScale, boolean flip) {
        final String[] coordinates = locationPos.split(" ");
        if (flip) {
            return convertCoordinates(Double.parseDouble(coordinates[1]), Double.parseDouble(coordinates[0]), locationSrsName, numberScale);
        } else {
            return convertCoordinates(Double.parseDouble(coordinates[0]), Double.parseDouble(coordinates[1]), locationSrsName, numberScale);
        }
    }

    public static Map<String, Object> putIntoSubMap(final Map<String, Object> map, final String subMapName, final String key, final Object value) {
        final Map<String, Object> subMap = (Map<String, Object>) map.computeIfAbsent(subMapName, (final String t) -> new HashMap<>());
        subMap.put(key, value);
        return subMap;
    }

    public static String afterLastSlash(final String input) {
        return input.substring(input.lastIndexOf('/') + 1);
    }

    public static PropertyBuilder propertiesBuilder() {
        return new PropertyBuilder();
    }

    public static class PropertyBuilder {

        Map<String, Object> properties = new HashMap<>();

        public PropertyBuilder addItem(final String key, final Object value) {
            properties.put(key, value);
            return this;
        }

        public PropertyBuilder addPath(final String path, final Object value) {
            CollectionsHelper.setOn(properties, path, value);
            return this;
        }

        public Map<String, Object> build() {
            return properties;
        }
    }

}
