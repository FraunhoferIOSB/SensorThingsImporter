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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.csv;

import de.fraunhofer.iosb.ilt.configurable.AnnotatedConfigurable;
import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyCodeSource;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import org.apache.commons.csv.CSVRecord;

/**
 *
 * @author hylke
 */
public class RecordConverterGroovy implements RecordConverter, AnnotatedConfigurable<SensorThingsService, Object> {

	@ConfigurableField(editor = EditorString.class,
			label = "Script", description = "The script that implements RecordConverter")
	@EditorString.EdOptsString(lines = 20,
			dflt = ""
			+ "Class Script implements RecordConverter {\n"
			+ "  public List<Observation> convert(CSVRecord record) throws ImportException {\n"
			+ "    // Your code here\n"
			+ "  }\n"
			+ "}")
	private String script;

	private final GroovyClassLoader gcl;
	private Class<RecordConverter> scriptClass;
	private RecordConverter scriptInstance;

	public RecordConverterGroovy() {
		gcl = new GroovyClassLoader(getClass().getClassLoader());
	}

	@Override
	public void init(SensorThingsService service) throws ImportException {
		try {
			GroovyCodeSource groovyCodeSource = new GroovyCodeSource(script, "RecordConverterClass", "/importer");
			scriptClass = gcl.parseClass(groovyCodeSource);
			scriptInstance = scriptClass.getDeclaredConstructor().newInstance();
			scriptInstance.init(service);
		} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
			throw new ImportException("Failed to create script instance", ex);
		}
	}

	@Override
	public List<Observation> convert(CSVRecord record) throws ImportException {
		return scriptInstance.convert(record);
	}

}
