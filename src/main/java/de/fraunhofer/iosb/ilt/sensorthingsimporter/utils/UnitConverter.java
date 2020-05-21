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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.utils;

import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBigDecimal;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorClass;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorList;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author hylke
 */
public class UnitConverter implements AnnotatedConfigurable<Object, Object> {

	public static class Unit implements AnnotatedConfigurable<Object, Object> {

		@ConfigurableField(editor = EditorString.class,
				label = "From", description = "The unit to convert from.")
		@EditorString.EdOptsString()
		private String from;

		@ConfigurableField(editor = EditorString.class,
				label = "To", description = "The unit to convert to.")
		@EditorString.EdOptsString()
		private String to;

		@ConfigurableField(editor = EditorBigDecimal.class,
				label = "Factor", description = "The factor to multiply 'From' with to get 'To'.")
		@EditorBigDecimal.EdOptsBigDecimal(dflt = 1)
		private BigDecimal factor;

		public Unit() {
		}

		public BigDecimal convert(BigDecimal from) {
			return from.multiply(factor);
		}

		public BigDecimal convertInverse(BigDecimal from) {
			return from.divide(factor);
		}
	}

	@ConfigurableField(editor = EditorList.class,
			label = "Conversions",
			description = "The list of unit conversions")
	@EditorList.EdOptsList(editor = EditorClass.class)
	@EditorClass.EdOptsClass(clazz = Unit.class)
	public List<Unit> conversions;

	public boolean inited = false;
	public Map<String, Map<String, Unit>> forward = new HashMap<>();
	public Map<String, Map<String, Unit>> reverse = new HashMap<>();

	public BigDecimal convert(String from, String to, BigDecimal value) {
		if (!inited) {
			init();
		}
		Unit unit = forward.computeIfAbsent(from, (t) -> Collections.emptyMap()).get(to);
		if (unit != null) {
			return unit.convert(value);
		}
		unit = reverse.computeIfAbsent(from, (t) -> Collections.emptyMap()).get(to);
		if (unit != null) {
			return unit.convertInverse(value);
		}
		return null;
	}

	private void init() {
		if (inited) {
			return;
		}
		for (Unit unit : conversions) {
			forward.computeIfAbsent(unit.from, (t) -> new HashMap<>()).put(unit.to, unit);
			reverse.computeIfAbsent(unit.to, (t) -> new HashMap<>()).put(unit.from, unit);
		}
	}
}
