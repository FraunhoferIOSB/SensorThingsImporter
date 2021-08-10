/*
 * Copyright (C) 2016 Fraunhofer Institut IOSB, Fraunhoferstr. 1, D 76131
 * Karlsruhe, Germany.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package de.fraunhofer.iosb.ilt.sensorthingsimporter.utils;

import org.geojson.Point;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author jab
 */
public class UtilsTest {

	static final double EXAMPLE_1_X = 350779.28125;
	static final double EXAMPLE_1_Y = 5815166.0;
	static final String EXAMPLE_1_SRS = "25832"; // Axis order = EAST, NORTH (X,Y)
	static final Point EXAMPLE_1_EXPECTED_GEOJSON = new Point(6.803341, 52.466243);

	static final double EXAMPLE_2_X = 16.51111;
	static final double EXAMPLE_2_Y = 48.145;
	static final String EXAMPLE_2_SRS = "urn:ogc:def:crs:EPSG::4326"; // AXIS ORDER = LAT,LON (Y,X)
	static final Point EXAMPLE_2_EXPECTED_GEOJSON = new Point(EXAMPLE_2_X, EXAMPLE_2_Y);

	@Test
	public void testCoordinateConversionXY() {
		// 25832 has axis order = EAST, NORTH (X,Y)
		Point point1 = FrostUtils.convertCoordinates(EXAMPLE_1_X, EXAMPLE_1_Y, EXAMPLE_1_SRS, 6);
		Assert.assertEquals("Points 1 not the same", EXAMPLE_1_EXPECTED_GEOJSON, point1);

		Point point2 = FrostUtils.convertCoordinates(EXAMPLE_2_Y, EXAMPLE_2_X, EXAMPLE_2_SRS, 6);
		Assert.assertEquals("Points 2 not the same", EXAMPLE_2_EXPECTED_GEOJSON, point2);
	}

	@Test
	public void testCoordinateConversionString() {
		Point point1 = FrostUtils.convertCoordinates(EXAMPLE_1_X + " " + EXAMPLE_1_Y, EXAMPLE_1_SRS, 6, false);
		Assert.assertEquals("Points 1 not the same", EXAMPLE_1_EXPECTED_GEOJSON, point1);

		Point point2 = FrostUtils.convertCoordinates(EXAMPLE_2_Y + " " + EXAMPLE_2_X, EXAMPLE_2_SRS, 6, false);
		Assert.assertEquals("Points 2 not the same", EXAMPLE_2_EXPECTED_GEOJSON, point2);
	}

}
