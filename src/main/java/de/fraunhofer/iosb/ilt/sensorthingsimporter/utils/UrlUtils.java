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

import com.fasterxml.jackson.core.type.TypeReference;
import de.fraunhofer.iosb.ilt.sensorthingsimporter.ImportException;
import de.fraunhofer.iosb.ilt.sta.Utils;
import de.fraunhofer.iosb.ilt.sta.jackson.ObjectMapperFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.ParseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
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

	public static final TypeReference<List<Map<String, Map<String, Object>>>> TYPE_LIST_MAP_MAP = new TypeReference<List<Map<String, Map<String, Object>>>>() {
	};

	/**
	 * The logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(UrlUtils.class);
	private static final Charset UTF8 = StandardCharsets.UTF_8;

	private UrlUtils() {
		// Utility class.
	}

	public static String fetchFromUrl(String targetUrl) throws IOException {
		return fetchFromUrl(targetUrl, UTF8);
	}

	public static String fetchFromUrl(String targetUrl, String charset) throws IOException {
		return fetchFromUrl(targetUrl, Charset.forName(charset));
	}

	public static String fetchFromUrl(String targetUrl, Charset charset) throws IOException {
		LOGGER.info("Fetching: {}", targetUrl);
		if (targetUrl.startsWith("file:/")) {
			return readFileUrl(targetUrl, charset);
		}
		return readNormalUrl(targetUrl, charset);
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

	public static String postJsonToUrl(String targetUrl, Object body, String username, String password) throws IOException {
		String queryBody = ObjectMapperFactory.get().writeValueAsString(body);
		try (CloseableHttpClient client = HttpClients.createSystem()) {
			HttpPost post = new HttpPost(targetUrl);
			if (!Utils.isNullOrEmpty(username) && !Utils.isNullOrEmpty(password)) {
				String auth = username + ":" + password;
				byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.ISO_8859_1));
				String authHeader = "Basic " + new String(encodedAuth);
				post.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
			}
			post.addHeader("Content-Type", "application/json");
			post.setEntity(new StringEntity(queryBody));
			CloseableHttpResponse response = client.execute(post);
			HttpEntity entity = response.getEntity();
			if (entity == null) {
				return "";
			}
			String data = EntityUtils.toString(entity, UTF8);
			return data;
		}

	}
}
