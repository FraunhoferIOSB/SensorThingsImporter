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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorEnum;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.math.BigDecimal;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author scf
 */
public class ParserBigdecimal implements Parser<BigDecimal>, AnnotatedConfigurable<SensorThingsService, Object> {

	public enum DecimalSeparator {
		POINT,
		COMMA,
		DETECT
	}
	@ConfigurableField(editor = EditorBoolean.class,
			label = "Truncate 0s", description = "Drop trailing zeroes from results.")
	@EditorBoolean.EdOptsBool()
	private boolean dropTailingZeroes;

	@ConfigurableField(editor = EditorEnum.class,
			label = "Decimal Separator", description = "What decimal separator should be used?")
	@EditorEnum.EdOptsEnum(sourceType = DecimalSeparator.class, dflt = "DETECT")
	private DecimalSeparator decimalSeparator;

	@Override
	public BigDecimal parse(JsonNode value) {
		try {
			BigDecimal bigDecimal = new BigDecimal(value.asText());
			if (dropTailingZeroes) {
				return bigDecimal.stripTrailingZeros();
			}
			return bigDecimal;
		} catch (NumberFormatException ex) {
			return null;
		}
	}

	@Override
	public BigDecimal parse(final String value) {
		String valueString = value.trim();
		switch (decimalSeparator) {
			case COMMA:
				valueString = convertFromComma(valueString);
				break;

			case POINT:
				// Nothing to do here.
				break;

			case DETECT:
				int idxComma = value.indexOf(',');
				int idxPoint = value.indexOf('.');
				if (idxComma > idxPoint) {
					valueString = convertFromComma(valueString);
				}
				break;
		}
		try {
			BigDecimal bigDecimal = new BigDecimal(valueString);
			if (dropTailingZeroes) {
				return bigDecimal.stripTrailingZeros();
			}
			return bigDecimal;
		} catch (NumberFormatException ex) {
			return null;
		}
	}

	private String convertFromComma(String valueString) {
		valueString = StringUtils.replace(valueString, ".", "");
		valueString = StringUtils.replace(valueString, ",", ".");
		return valueString;
	}
}
