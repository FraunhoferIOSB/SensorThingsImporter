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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.auth;

import de.fraunhofer.iosb.ilt.configurable.annotations.ConfigurableField;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorBoolean;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorPassword;
import de.fraunhofer.iosb.ilt.configurable.editor.EditorString;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.IOException;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.LoggerFactory;

/**
 *
 * @author scf
 */
public class AuthPostCookie implements AuthMethod {

	public static final String HTTPREQUEST_HEADER_ACCEPT = "Accept";
	public static final String HTTPREQUEST_HEADER_CONTENT_TYPE = "Content-Type";
	public static final String HTTPREQUEST_TYPE_JSON = "application/json";
	/**
	 * The logger for this class.
	 */
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(AuthPostCookie.class);

	@ConfigurableField(editor = EditorString.class,
			label = "PostUrl",
			description = "The url to post to, use placeholders {username} and {password} for username and password.",
			optional = false
	)
	@EditorString.EdOptsString(dflt = "https://example.org/servlet/is/rest/login?user={username}&key={password}")
	private String postUrl;

	@ConfigurableField(editor = EditorString.class,
			label = "Username",
			description = "The username to use for authentication",
			optional = false
	)
	@EditorString.EdOptsString()
	private String username;

	@ConfigurableField(editor = EditorPassword.class,
			label = "Password",
			description = "The password to use for authentication",
			optional = false)
	@EditorPassword.EdOptsPassword()
	private String password;

	@ConfigurableField(editor = EditorBoolean.class,
			label = "IgnoreSslErrors",
			description = "Ignore SSL certificate errors. This is a bad idea unless you know what you are doing.",
			optional = true)
	@EditorBoolean.EdOptsBool()
	private boolean ignoreSslErrors;

	@Override
	public void setAuth(SensorThingsService service) {
		String finalUrl = postUrl.replace("{username}", username);
		finalUrl = finalUrl.replace("{password}", password);
		CloseableHttpClient client = service.getHttpClient();
		final HttpPost loginPost = new HttpPost(finalUrl);
		loginPost.setHeader(HTTPREQUEST_HEADER_ACCEPT, HTTPREQUEST_TYPE_JSON);
		try {
			CloseableHttpResponse response = client.execute(loginPost);
			final StatusLine statusLine = response.getStatusLine();
			LOGGER.debug("Response: {}, {}", statusLine.getStatusCode(), statusLine.getReasonPhrase());
			if (statusLine.getStatusCode() >= 300) {
				LOGGER.error("Login failed: {},{}\n{}", statusLine.getStatusCode(), statusLine.getReasonPhrase(), EntityUtils.toString(response.getEntity()));
			}
		} catch (IOException ex) {
			LOGGER.error("Failed to login.", ex);
		}
	}

}
