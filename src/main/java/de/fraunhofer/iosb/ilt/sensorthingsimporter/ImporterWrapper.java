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
package de.fraunhofer.iosb.ilt.sensorthingsimporter;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import de.fraunhofer.iosb.ilt.configurable.Configurable;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorMap;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorSubclass;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.auth.AuthMethod;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.csv.Options;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class ImporterWrapper implements Configurable<Object, Object> {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(ImporterWrapper.class);

	private EditorMap<Object, Object, Map<String, Object>> editor;
	private EditorString editorService;
	private EditorSubclass<Object, Object, AuthMethod> editorAuthMethod;
	private EditorSubclass<Object, Object, Importer> editorImporter;

	private boolean noAct = false;
	private Importer importer;

	@Override
	public void configure(JsonElement config, Object context, Object edtCtx) {
		SensorThingsService service = new SensorThingsService();
		getConfigEditor(service, edtCtx).setConfig(config, service, edtCtx);
		try {
			service.setEndpoint(new URI(editorService.getValue()));
			AuthMethod authMethod = editorAuthMethod.getValue();
			if (authMethod != null) {
				authMethod.setAuth(service);
			}
		} catch (URISyntaxException ex) {
			LOGGER.error("Failed to create service.", ex);
			throw new IllegalArgumentException("Failed to create service.", ex);
		}
		importer = editorImporter.getValue();
	}

	@Override
	public EditorMap<Object, Object, Map<String, Object>> getConfigEditor(Object context, Object edtCtx) {
		if (editor == null) {
			editor = new EditorMap<>();
			editorService = new EditorString("https://service.somewhere/path/v1.0", 1, "Service URL", "The url of the server.");
			editor.addOption("serviceUrl", editorService, false);

			editorAuthMethod = new EditorSubclass<>(AuthMethod.class, "Auth Method", "The authentication method the service uses.", false, "className");
			editor.addOption("authMethod", editorAuthMethod, true);

			editorImporter = new EditorSubclass(Importer.class, "Importer", "The specific importer to use.", false, "className");
			editor.addOption("importer", editorImporter, false);
		}
		return editor;
	}

	public void doImport(Options options) {
		this.noAct = options.getNoAct().isSet();
		String fileName = options.getFileName().getValue();
		File configFile = new File(fileName);
		try {
			JsonElement json = new JsonParser().parse(new FileReader(configFile));
			configure(json, null, null);
			importer.setNoAct(noAct);
			importer.doImport();

		} catch (JsonSyntaxException exc) {
			LOGGER.debug("Failed to parse config.", exc);
		} catch (ImportException exc) {
			LOGGER.trace("Failed to import.", exc);
		} catch (FileNotFoundException ex) {
			LOGGER.error("Failed to read config file.", ex);
		}
	}

	public void doImport(String config, boolean noAct) {
		this.noAct = noAct;
		try {
			JsonElement json = new JsonParser().parse(config);
			configure(json, null, null);
			importer.setNoAct(noAct);
			importer.doImport();
		} catch (JsonSyntaxException exc) {
			LOGGER.error("Failed to parse {}", config);
			LOGGER.debug("Failed to parse.", exc);
		} catch (ImportException exc) {
			LOGGER.trace("Failed to import.", exc);
		}
	}

	public static void importConfig(String config, boolean noAct) {
		ImporterWrapper wrapper = new ImporterWrapper();
		wrapper.doImport(config, noAct);
	}

	public static void importCmdLine(List<String> arguments) throws URISyntaxException, IOException, MalformedURLException, ServiceFailureException {
		Options options = new Options().parseArguments(arguments);

		ImporterWrapper wrapper = new ImporterWrapper();
		wrapper.doImport(options);
	}

}
