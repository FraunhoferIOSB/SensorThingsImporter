/*
 * Copyright (C) 2019 Fraunhofer IOSB
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

import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class UrlUtils {

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(UrlUtils.class);
	private static final Charset UTF8 = Charset.forName("UTF-8");

	private UrlUtils() {
		// Utility class.
	}

	public static String fetchFromUrl(String targetUrl) throws ImportException {
		return fetchFromUrl(targetUrl, UTF8);
	}

	public static String fetchFromUrl(String targetUrl, String charset) throws ImportException {
		return fetchFromUrl(targetUrl, Charset.forName(charset));
	}

	public static String fetchFromUrl(String targetUrl, Charset charset) throws ImportException {
		LOGGER.info("Fetching: {}", targetUrl);
		try {
			if (targetUrl.startsWith("file://")) {
				return readFileUrl(targetUrl, charset);
			}
			return readNormalUrl(targetUrl, charset);
		} catch (IOException ex) {
			LOGGER.error("Failed to fetch url {}: {}", targetUrl, ex.getMessage());
			throw new ImportException("Failed to fetch url " + targetUrl, ex);
		}
	}

	private static String readNormalUrl(String targetUrl, Charset charset) throws IOException, ParseException {
		try (CloseableHttpClient client = HttpClients.createSystem()) {
			HttpGet get = new HttpGet(targetUrl);
			CloseableHttpResponse response = client.execute(get);
			HttpEntity entity = response.getEntity();
			if (entity == null) {
				return "";
			}
			String data = EntityUtils.toString(entity, charset);
			return data;
		}
	}

	private static String readFileUrl(String targetUrl, Charset charset) throws IOException {
		try (InputStream input = new URL(targetUrl).openStream()) {
			String string = IOUtils.toString(input, charset);
			return string;
		}
	}

}
