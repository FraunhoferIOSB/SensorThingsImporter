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
package de.fraunhofer.iosb.ilt.sensorthingsimporter.utils;

import de.fraunhofer.iosb.ilt.sta.Utils;
import de.fraunhofer.iosb.ilt.sta.jackson.ObjectMapperFactory;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.ParseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.type.TypeReference;

public class UrlUtils {

    public static final TypeReference<List<Map<String, Map<String, Object>>>> TYPE_LIST_MAP_MAP = new TypeReference<List<Map<String, Map<String, Object>>>>() {
        // empty by design
    };

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(UrlUtils.class);
    public static final Charset UTF8 = StandardCharsets.UTF_8;

    private UrlUtils() {
        // Utility class.
    }

    public static HttpResponse fetchFromUrl(String targetUrl) throws IOException {
        return fetchFromUrl(targetUrl, UTF8);
    }

    public static HttpResponse fetchFromUrl(String targetUrl, String charset) throws IOException {
        return fetchFromUrl(targetUrl, Charset.forName(charset));
    }

    public static HttpResponse fetchFromUrl(String targetUrl, Charset charset) throws IOException {
        if (targetUrl.startsWith("file:/")) {
            return readFileUrl(targetUrl, charset);
        }
        return readNormalUrl(targetUrl, charset, Collections.emptyList(), null, null);
    }

    public static HttpResponse readNormalUrl(String targetUrl, Charset charset) throws IOException, ParseException {
        return readNormalUrl(targetUrl, charset, Collections.emptyList(), null, null);
    }

    public static HttpResponse readNormalUrl(String targetUrl, Charset charset, List<Header> headers, String username, String password) throws IOException, ParseException {
        LOGGER.debug("Fetching: {}", targetUrl);
        HttpClientBuilder clientBuilder = HttpClientBuilder.create()
                .useSystemProperties()
                .setDefaultRequestConfig(
                        RequestConfig.custom()
                                .setSocketTimeout(20_000)
                                .setConnectTimeout(10_000)
                                .setConnectionRequestTimeout(10_000)
                                .build());
        CloseableHttpClient client = clientBuilder.build();
        HttpGet get = new HttpGet(targetUrl);
        if (!Utils.isNullOrEmpty(username) && !Utils.isNullOrEmpty(password)) {
            String auth = username + ":" + password;
            byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.ISO_8859_1));
            String authHeader = "Basic " + new String(encodedAuth);
            get.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
        }
        boolean hasAccept = false;
        for (var h : headers) {
            if ("accept".equalsIgnoreCase(h.getName())) {
                hasAccept = true;
            }
            get.addHeader(h);
        }
        if (!hasAccept) {
            get.addHeader("Accept", "*/*");
        }
        CloseableHttpResponse response = client.execute(get);
        HttpEntity entity = response.getEntity();
        final int statusCode = response.getStatusLine().getStatusCode();
        LOGGER.info("Fetched:  {}: {}", targetUrl, statusCode);
        return new HttpResponse(client, targetUrl, statusCode, entity, response.getAllHeaders());
    }

    private static HttpResponse readFileUrl(String targetUrl, Charset charset) throws IOException {
        LOGGER.info("Loading: {}", targetUrl);
        InputStream input = new URL(targetUrl).openStream();
        if (input.markSupported()) {
            input.mark(4);
            byte[] firstBytes = new byte[3];
            input.read(firstBytes);
            charset = guessCharset(firstBytes, charset);
            input.reset();
        }
        return new HttpResponse(
                null,
                targetUrl,
                200,
                new InputStreamEntity(input, ContentType.DEFAULT_TEXT.withCharset(charset)));
    }

    public static HttpResponse postJsonToUrl(String targetUrl, Object body, String username, String password) throws IOException {
        return postJsonToUrl(targetUrl, body, Collections.emptyList(), username, password);
    }

    public static HttpResponse postJsonToUrl(String targetUrl, Object body, List<Header> headers, String username, String password) throws IOException {
        LOGGER.info("Posting: {}", targetUrl);
        final String queryBody = ObjectMapperFactory.get().writeValueAsString(body);
        return postToUrl(targetUrl, headers, queryBody, username, password);
    }

    public static HttpResponse postToUrl(String targetUrl, String queryBody, String username, String password) throws IOException, ParseException {
        return postToUrl(targetUrl, Collections.EMPTY_LIST, queryBody, username, password);
    }

    public static HttpResponse postToUrl(String targetUrl, List<Header> headers, String queryBody, String username, String password) throws IOException, ParseException {
        HttpClientBuilder builder = HttpClientBuilder.create()
                .useSystemProperties()
                .setMaxConnTotal(1)
                .setDefaultRequestConfig(
                        RequestConfig.custom()
                                .setSocketTimeout(1000 * 60 * 15)
                                .build());
        CloseableHttpClient client = builder.build();
        final HttpPost post = new HttpPost(targetUrl);
        if (!Utils.isNullOrEmpty(username) && !Utils.isNullOrEmpty(password)) {
            final String auth = username + ":" + password;
            final byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.ISO_8859_1));
            final String authHeader = "Basic " + new String(encodedAuth);
            post.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
        }
        boolean hasAccept = false;
        boolean hasContentType = false;
        for (var h : headers) {
            if ("accept".equalsIgnoreCase(h.getName())) {
                hasAccept = true;
            }
            post.addHeader(h);
        }
        if (!hasAccept) {
            post.addHeader("Accept", "*/*");
        }
        if (!hasContentType) {
            post.addHeader("Content-Type", "application/json");
        }
        post.setEntity(new StringEntity(queryBody));
        LOGGER.debug("Posting to {}", targetUrl);
        LOGGER.trace("Posting:\n{}", queryBody);
        final CloseableHttpResponse response = client.execute(post);
        final HttpEntity entity = response.getEntity();
        final int statusCode = response.getStatusLine().getStatusCode();
        if (entity == null) {
            LOGGER.debug("Response: {}, no data", statusCode);
        }
        return new HttpResponse(client, targetUrl, statusCode, entity, response.getAllHeaders());

    }

    public static class HttpResponse implements AutoCloseable {

        public final String url;
        public final int code;
        private final HttpEntity data;
        private final CloseableHttpClient client;
        public final Map<String, String> headers;

        public HttpResponse(CloseableHttpClient client, String url, int code, HttpEntity data) {
            this(client, url, code, data, null);
        }

        public HttpResponse(CloseableHttpClient client, String url, int code, HttpEntity data, Header[] headers) {
            this.client = client;
            this.url = url;
            this.code = code;
            this.data = data;
            this.headers = new LinkedHashMap<>();
            if (headers != null) {
                for (Header header : headers) {
                    this.headers.put(header.getName().toLowerCase(), header.getValue());
                }
            }
        }

        public HttpEntity getData() {
            return data;
        }

        public String getDataString() throws IOException {
            return EntityUtils.toString(data, UTF8);
        }

        public Reader getDataReader() throws IOException {
            ContentType contentType = null;
            Charset charset = null;
            try {
                contentType = ContentType.get(data);
            } catch (final UnsupportedCharsetException ex) {
            }
            if (contentType != null) {
                charset = contentType.getCharset();
            }
            if (charset == null) {
                charset = UTF8;
            }
            final InputStream inStream = data.getContent();
            if (inStream == null) {
                return null;
            }
            return new InputStreamReader(inStream, charset);
        }

        public InputStream getDataBinary() throws IOException {
            return data.getContent();
        }

        public String getUrl() {
            return url;
        }

        public boolean isOkResponse() {
            return code >= 200 && code < 300;
        }

        public boolean isRedirect() {
            return code >= 300 && code < 400;
        }

        public boolean isError() {
            return code >= 400;
        }

        @Override
        public void close() throws IOException {
            EntityUtils.consumeQuietly(data);
            if (client != null) {
                client.close();
            }
        }

    }

    private static final byte[] BOM_UTF16_LE = new byte[]{(byte) 0xFF, (byte) 0xFE};
    private static final byte[] BOM_UTF16_BE = new byte[]{(byte) 0xFE, (byte) 0xFF};
    private static final byte[] BOM_UTF8 = new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};

    public static String entityToString(HttpEntity entity, Charset charset) throws IOException {
        ContentType contentType = ContentType.get(entity);
        if (contentType != null) {
            if (contentType.getCharset() != null) {
                return EntityUtils.toString(entity, charset);
            }
        }
        try (InputStream content = entity.getContent(); BufferedInputStream bufContent = new BufferedInputStream(content)) {
            byte[] firstBytes = new byte[3];
            bufContent.mark(3);
            bufContent.read(firstBytes);
            bufContent.reset();
            return IOUtils.toString(bufContent, guessCharset(firstBytes, charset));
        }
    }

    private static Charset guessCharset(byte[] firstThreeBytes, Charset deflt) {
        if (startsWith(firstThreeBytes, BOM_UTF16_LE)) {
            return StandardCharsets.UTF_16LE;
        }
        if (startsWith(firstThreeBytes, BOM_UTF16_BE)) {
            return StandardCharsets.UTF_16BE;
        }
        if (startsWith(firstThreeBytes, BOM_UTF8)) {
            return StandardCharsets.UTF_8;
        }
        return deflt;
    }

    private static boolean startsWith(byte[] content, byte[] probe) {
        if (probe.length > content.length) {
            return false;
        }
        for (int i = 0; i < probe.length; i++) {
            if (content[i] != probe[i]) {
                return false;
            }
        }
        return true;
    }
}
