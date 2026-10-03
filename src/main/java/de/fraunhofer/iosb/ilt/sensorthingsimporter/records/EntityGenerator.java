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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.records;

import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.frostclient.SensorThingsService;
import de.fraunhofer.iosb.ilt.frostclient.exception.Exceptions;
import de.fraunhofer.iosb.ilt.frostclient.exception.ServiceFailureException;
import de.fraunhofer.iosb.ilt.frostclient.exception.StatusCodeException;
import de.fraunhofer.iosb.ilt.frostclient.model.Entity;
import de.fraunhofer.iosb.ilt.frostclient.model.EntityType;
import de.fraunhofer.iosb.ilt.frostclient.model.ModelRegistry;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.EntityCache;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.EntityCache.PropertyExtractor;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.Translator;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates any entity.
 */
public class EntityGenerator implements AnnotatedConfigurable<Object, Object> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EntityGenerator.class.getName());

    @ConfigurableField(editor = EditorString.class, optional = false,
            label = "EntityType", description = "The name of the entity type")
    @EditorString.EdOptsString(lines = 1, dflt = "ObservedProperty")
    private String entityTypeName;

    @ConfigurableField(editor = EditorString.class, optional = false,
            label = "Body Template", description = "Template used to generate the body of the post, using {path/to/field|default} placeholders.")
    @EditorString.EdOptsString(lines = 6)
    private String templateBody;

    @ConfigurableField(editor = EditorString.class, optional = false,
            label = "EqualsFilter", description = "Template used to generate the STA-filter to check for duplicates, using {path/to/field|default} placeholders. Template runs against the new Entity!")
    @EditorString.EdOptsString(lines = 1, dflt = "name eq '{name}'")
    private String templateEqualsFilter;

    @ConfigurableField(editor = EditorString.class, optional = false,
            label = "CacheKey", description = "Template used to generate the key used to cache, using {path/to/field|default} placeholders. Template runs against the new Entity!")
    @EditorString.EdOptsString(lines = 1, dflt = "{id}")
    private String templateCacheKey;

    private SensorThingsService service;
    private EntityType entityType;
    private EntityCache<String> cache;
    private PropertyExtractor<String> localIdExtractor;
    private PropertyExtractor<String> nameExtractor;

    public void init(SensorThingsService service) {
        this.service = service;
        final ModelRegistry mr = service.getModelRegistry();
        entityType = mr.getEntityTypeForName(entityTypeName);
        Exceptions.illegalArgumentIf(entityType == null, "EntityType {} not found in model registry.", entityTypeName);

        localIdExtractor = entity -> Translator.fillTemplate(templateCacheKey, entity, Translator.StringType.PLAIN, true);
        nameExtractor = entity -> Translator.fillTemplate(templateCacheKey, entity, Translator.StringType.PLAIN, true);
        cache = new EntityCache<>(localIdExtractor, nameExtractor);
    }

    public Entity getFor(Tuple tuple) {
        String bodyString = Translator.fillTemplate(templateBody, tuple);
        try {
            Entity genEntity = service.getJsonReader().parseEntity(entityType, bodyString);
            String localId = localIdExtractor.extractFrom(genEntity);
            Entity cachedEntity = cache.get(localId);
            if (cachedEntity != null) {
                return cachedEntity;
            }
            String filter = Translator.fillTemplate(templateEqualsFilter, genEntity, Translator.StringType.URL);
            int foundCount = cache.load(service.dao(entityType), filter);
            if (foundCount > 1) {
                LOGGER.warn("Found multiple {} for filter {}", entityType, filter);
            }
            cachedEntity = cache.get(localId);
            if (cachedEntity != null) {
                return cachedEntity;
            }
            service.create(genEntity);
            LOGGER.info("Created {} for filter {}.", genEntity, filter);
            cache.add(genEntity);
            return genEntity;

        } catch (IOException ex) {
            throw new IllegalArgumentException("Failed to generate entity from JSON", ex);
        } catch (StatusCodeException ex) {
            LOGGER.error("Failed to create entity: {}\n{}", ex.getStatusCode(), ex.getReturnedContent());
            throw new ImportException("Failed to create entity");
        } catch (ServiceFailureException ex) {
            throw new ImportException("Failed to create entity", ex);
        }
    }

    public String getEntityTypeName() {
        return entityTypeName;
    }

    public void setEntityTypeName(String entityTypeName) {
        this.entityTypeName = entityTypeName;
    }

    public String getTemplateBody() {
        return templateBody;
    }

    public void setTemplateBody(String templateBody) {
        this.templateBody = templateBody;
    }

    public String getTemplateEqualsFilter() {
        return templateEqualsFilter;
    }

    public void setTemplateEqualsFilter(String templateEqualsFilter) {
        this.templateEqualsFilter = templateEqualsFilter;
    }

    public String getTemplateCacheKey() {
        return templateCacheKey;
    }

    public void setTemplateCacheKey(String templateCacheKey) {
        this.templateCacheKey = templateCacheKey;
    }

}
