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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.timegen;

import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorEnum;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorInt;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.MultiDatastream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 *
 * @author scf
 */
public class TimeGenAgo implements TimeGen {

	@ConfigurableField(editor = EditorInt.class,
			label = "Amount", description = "The amount of steps to go into the past.")
	@EditorInt.EdOptsInt(dflt = 1, min = 0, max = 999999, step = 1)
	private int amount;
	@ConfigurableField(editor = EditorEnum.class,
			label = "Unit", description = "The unit.")
	@EditorEnum.EdOptsEnum(sourceType = ChronoUnit.class, dflt = "DAYS")
	private ChronoUnit unit;

	@Override
	public Instant getInstant() {
		return Instant.now().minus(unit.getDuration().multipliedBy(amount));
	}

	@Override
	public Instant getInstant(Datastream ds) {
		return Instant.now().minus(unit.getDuration().multipliedBy(amount));
	}

	@Override
	public Instant getInstant(MultiDatastream mds) {
		return Instant.now().minus(unit.getDuration().multipliedBy(amount));
	}

}
