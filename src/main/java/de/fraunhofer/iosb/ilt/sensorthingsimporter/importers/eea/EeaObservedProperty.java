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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea;

import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.TAG_LOCAL_ID;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.TAG_OWNER;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.TAG_RECOMMENDED_UNIT;
import static de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea.EeaConstants.VALUE_OWNER_EEA;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.EntityCache;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.FrostUtils;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.Utils;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *
 * @author hylke
 */
public class EeaObservedProperty {

	private static class EeaOp {

		int localId;
		String name;
		String description;
		String definition;
		String recommendedUnit;

		public EeaOp(int localId, String name, String description, String recommendedUnit, String definition) {
			this.localId = localId;
			this.name = name;
			this.description = description;
			this.recommendedUnit = recommendedUnit;
			this.definition = definition;
		}
	}

	private static class EeaOpRegistry {

		private final Map<Integer, EeaOp> atOpById = new HashMap<>();

		public void register(EeaOp atop) {
			atOpById.put(atop.localId, atop);
		}

		public Collection<EeaOp> values() {
			return atOpById.values();
		}
	}

	public static EntityCache<String, ObservedProperty> createObservedPropertyCache() {
		EntityCache<String, ObservedProperty> observedPropertyCache = new EntityCache<>(
				entity -> Objects.toString(entity.getProperties().get(TAG_LOCAL_ID)),
				ObservedProperty::getName);
		return observedPropertyCache;
	}

	public static void importObservedProperties(FrostUtils frostUtils, EntityCache<String, ObservedProperty> observedPropertyCache) throws ServiceFailureException {
		EeaOpRegistry opRegistry = new EeaOpRegistry();
		opRegistry.register(new EeaOp(1, "SO2", "SO2", "µg/m3", "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1"));
		opRegistry.register(new EeaOp(5, "PM10", "PM10", "µg/m3", "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5"));
		opRegistry.register(new EeaOp(7, "O3", "O3", "µg/m3", "http://dd.eionet.europa.eu/vocabularyconcept/aq/pollutant/7"));
		opRegistry.register(new EeaOp(8, "NO2", "NO2", "µg/m3", "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/8"));
		opRegistry.register(new EeaOp(9, "NOX as NO2", "NOX as NO2", "µg/m3", "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/9"));
		opRegistry.register(new EeaOp(10, "CO", "CO", "mg/m3", "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/10"));
		opRegistry.register(new EeaOp(38, "NO", "NO", "µg/m3", "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/38"));
		opRegistry.register(new EeaOp(71, "CO2", "CO2", "ppmv", "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/71"));
		opRegistry.register(new EeaOp(6001, "PM2.5", "PM2.5", "µg/m3", "http://dd.eionet.europa.eu/vocabulary/aq/pollutant/6001"));

		for (EeaOp atop : opRegistry.values()) {
			Map<String, Object> properties = new HashMap<>();
			properties.put(TAG_LOCAL_ID, atop.localId);
			properties.put(TAG_OWNER, VALUE_OWNER_EEA);
			properties.put(TAG_RECOMMENDED_UNIT, atop.recommendedUnit);

			String filter = "properties/" + TAG_LOCAL_ID + " eq " + Utils.quoteForUrl(atop.localId);
			ObservedProperty cachedObservedProperty = observedPropertyCache.get(Integer.toString(atop.localId));
			ObservedProperty op = frostUtils.findOrCreateOp(filter, atop.name, atop.definition, atop.description, properties, cachedObservedProperty);
			observedPropertyCache.add(op);
		}
	}
}
