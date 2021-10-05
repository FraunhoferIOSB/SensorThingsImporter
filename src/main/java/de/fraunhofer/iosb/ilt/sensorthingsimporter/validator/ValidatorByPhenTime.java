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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.validator;

import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ObservationUploader;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.dao.BaseDao;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.TimeObject;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.math.BigDecimal;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class ValidatorByPhenTime implements Validator, AnnotatedConfigurable<SensorThingsService, Object> {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ValidatorByPhenTime.class);

	@ConfigurableField(editor = EditorBoolean.class,
			label = "Update", description = "Update results that are different.")
	@EditorBoolean.EdOptsBool()
	private boolean update;

	@ConfigurableField(editor = EditorBoolean.class,
			label = "Cache", description = "Download & cache all observations with phenomenonTime later than the first encoutered.")
	@EditorBoolean.EdOptsBool()
	private boolean cacheObservations;

	@ConfigurableField(editor = EditorBoolean.class,
			label = "Cache deletes duplicates", description = "Delete duplicates if the cache encouters them.")
	@EditorBoolean.EdOptsBool(dflt = true)
	private boolean deleteDuplicates;

	private ObservationUploader uploader;

	private final ThreadLocal<ObsCache> cacheHolder = new ThreadLocal<>();

	@Override
	public void setObservationUploader(ObservationUploader uploader) {
		this.uploader = uploader;
	}

	private ObsCache getCache() {
		ObsCache cache = cacheHolder.get();
		if (cache == null) {
			cache = new ObsCache(uploader, deleteDuplicates);
			cacheHolder.set(cache);
		}
		return cache;
	}

	private boolean resultCompare(Object one, Object two) {
		if (one == null) {
			return two == null;
		}
		if (two == null) {
			return false;
		}
		if (one.equals(two)) {
			return true;
		}
		if (one instanceof List) {
			if (!(two instanceof List)) {
				return false;
			}
			List listOne = (List) one;
			List listTwo = (List) two;
			int size = listOne.size();
			if (listTwo.size() != size) {
				return false;
			}
			for (int i = 0; i < size; i++) {
				if (!resultCompare(listOne.get(i), listTwo.get(i))) {
					return false;
				}
			}
			return true;
		}
		try {
			if (one instanceof Long && two instanceof Integer) {
				return one.equals(Long.valueOf((Integer) two));
			}
			if (two instanceof Long && one instanceof Integer) {
				return two.equals(Long.valueOf((Integer) one));
			}
			if (one instanceof BigDecimal && two instanceof BigDecimal) {
				// Would have returned true above if equal
				return false;
			}
			if (one instanceof BigDecimal) {
				return ((Comparable<BigDecimal>) one).compareTo(new BigDecimal(two.toString())) == 0;
			}
			if (two instanceof BigDecimal) {
				return ((Comparable<BigDecimal>) two).compareTo(new BigDecimal(one.toString())) == 0;
			}
		} catch (NumberFormatException e) {
			LOGGER.trace("Not both bigdecimal.", e);
			// not both bigDecimal.
		}
		return false;
	}

	private BaseDao<Observation> validateCache(Datastream d, MultiDatastream m) {
		if (cacheObservations) {
			if (d != null) {
				getCache().clearIfDifferent(d.getId());
			}
			if (m != null) {
				getCache().clearIfDifferent(m.getId());
			}
		}
		if (d != null) {
			return d.observations();
		}
		if (m != null) {
			return m.observations();
		}
		throw new IllegalArgumentException("Must pass either a Datastream or multiDatastream.");
	}

	private Observation getObservation(TimeObject phenTime, BaseDao<Observation> observations) throws ServiceFailureException {
		if (cacheObservations) {
			return getCache().getFromCache(phenTime, observations);
		}
		return observations.query().select("@iot.id", "result").filter("phenomenonTime eq " + phenTime.toString()).first();
	}

	private void addToCache(Observation obs) {
		if (cacheObservations) {
			getCache().put(obs.getPhenomenonTime(), obs);
		}
	}

	@Override
	public boolean isValid(Observation obs) throws ImportException {
		try {
			Datastream d = obs.getDatastream();
			MultiDatastream m = obs.getMultiDatastream();
			BaseDao<Observation> observations = validateCache(d, m);

			TimeObject phenomenonTime = obs.getPhenomenonTime();
			Observation first = getObservation(phenomenonTime, observations);
			if (first == null) {
				addToCache(obs);
				return true;
			} else {
				if (!resultCompare(obs.getResult(), first.getResult())) {
					LOGGER.debug("Observation {} with given phenomenonTime {} exists, but result not the same. {} != {} .", first.getId(), phenomenonTime, obs.getResult(), first.getResult());
					if (update) {
						obs.setId(first.getId());
						addToCache(obs);
						return true;
					}
				}
				return false;
			}
		} catch (ServiceFailureException ex) {
			LOGGER.debug("Exception fetching validation observations: {}", ex.getMessage());
			throw new ImportException("Failed to validate.", ex);
		}
	}

}
