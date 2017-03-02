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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.query.Query;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;

/**
 *
 * @author scf
 */
public class DsMapperFilter implements DatastreamMapper {

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(DsMapperFilter.class);
    private final SensorThingsService service;
    private final Map<String, Datastream> datastreamCache = new HashMap<>();
    private final String filterTemplate;
    private final Pattern placeHolderPattern = Pattern.compile("\\{([0-9]+)\\}");
    private final Matcher matcher;

    public DsMapperFilter(SensorThingsService service, String filterTemplate) {
        this.service = service;
        this.filterTemplate = filterTemplate;
        matcher = placeHolderPattern.matcher(filterTemplate);
    }

    @Override
    public Datastream getDatastreamFor(CSVRecord record) {
        try {
            String filter = fillTemplate(record);
            Datastream ds = getDatastreamFor(filter);
            return ds;
        } catch (ServiceFailureException ex) {
            LOGGER.error("Failed to fetch datastream.", ex);
            throw new IllegalArgumentException(ex);
        }
    }

    public Datastream getDatastreamFor(String filter) throws ServiceFailureException {
        Datastream ds = datastreamCache.get(filter);
        if (ds != null) {
            return ds;
        }
        Query<Datastream> query = service.datastreams().query().filter(filter);
        EntityList<Datastream> streams = query.list();
        if (streams.size() != 1) {
            LOGGER.error("Found incorrect number of datastreams: {}", streams.size());
            throw new IllegalArgumentException("Found incorrect number of datastreams: " + streams.size());
        }
        ds = streams.iterator().next();
        LOGGER.info("Found datastream {} for query {}.", ds.getId(), filter);
        datastreamCache.put(filter, ds);
        return ds;
    }

    private String fillTemplate(CSVRecord record) {
        matcher.reset();
        StringBuilder filter = new StringBuilder();
        int pos = 0;
        while (matcher.find()) {
            int start = matcher.start();
            filter.append(filterTemplate.substring(pos, start));
            int colNr = Integer.parseInt(matcher.group(1));
            filter.append(record.get(colNr));
            pos = matcher.end();
        }
        filter.append(filterTemplate.substring(pos));
        return filter.toString();
    }
}
