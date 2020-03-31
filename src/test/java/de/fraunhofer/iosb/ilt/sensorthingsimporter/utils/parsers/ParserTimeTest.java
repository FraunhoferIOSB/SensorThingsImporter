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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.utils.parsers;

import java.time.ZonedDateTime;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author hylke
 */
public class ParserTimeTest {

	public ParserTimeTest() {
	}

	/**
	 * Test of parse method, of class ParserTime.
	 */
	@Test
	public void testParse_String() {
		ParserTime instance = new ParserTime();
		instance.setFormat("yyyy-MM-dd HH:mm:ssXXX");
		instance.setZone("");

		ZonedDateTime expResult = ZonedDateTime.parse("2020-03-29T00:00:00+01:00");
		ZonedDateTime result = instance.parse("2020-03-29 00:00:00+01:00");
		Assert.assertEquals(expResult, result);
	}

}
