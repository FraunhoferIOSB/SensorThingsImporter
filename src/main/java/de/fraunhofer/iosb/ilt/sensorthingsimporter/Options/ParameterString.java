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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.Options;

/**
 *
 * @author scf
 */
public class ParameterString implements Parameter<String> {

    private final String name;
    private String value;

    /**
     * @param name The name of the parameter used in the help.
     * @param value The default value.
     */
    public ParameterString(String name, String value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public String parse(String arg) {
        value = arg;
        return value;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getName() {
        return name;
    }

}
