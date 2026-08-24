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

/**
 * A record or Tuple-
 */
public interface Tuple {

    /**
     * Check if the given name exists as a field (or column).
     *
     * @param name The name to check for.
     * @return true if the name matches a field or column.
     */
    public boolean isMapped(String name);

    /**
     * Get the value at the given index.
     *
     * @param idx the index to check for.
     * @return The string value at the given index.
     */
    public String getString(int idx);

    /**
     * Get the value mapped to the given name.
     *
     * @param name The name of the field/column to get the value for.
     * @return the string value at the given mapped name.
     */
    public String getString(String name);

}
