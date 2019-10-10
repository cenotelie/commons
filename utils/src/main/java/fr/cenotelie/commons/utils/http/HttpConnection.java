/*******************************************************************************
 * Copyright (c) 2016 Association Cénotélie (cenotelie.fr)
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General
 * Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/

package fr.cenotelie.commons.utils.http;

import fr.cenotelie.commons.utils.Base64;
import fr.cenotelie.commons.utils.IOUtils;
import fr.cenotelie.commons.utils.collections.Couple;
import fr.cenotelie.commons.utils.logging.Logging;

import javax.net.ssl.*;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;

/**
 * Represents a basic HTTP connection
 *
 * @author Laurent Wouters
 */
public class HttpConnection implements Closeable {
    /**
     * The default user agent
     */
    public static final String USER_AGENT_DEFAULT = "Mozilla/5.0 (X11; Linux x86_64) HttpConnection/1.0";

    /**
     * Represents a trust manager that accepts all certificates
     */
    private static final TrustManager TRUST_MANAGER_ACCEPT_ALL = new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    };

    /**
     * Represents a hostname verifier that accepts all hosts
     */
    private static final HostnameVerifier HOSTNAME_VERIFIER_ACCEPT_ALL = (s, sslSession) -> true;
    /**
     * The SSL context for HTTPS connections
     */
    private final SSLContext sslContext;
    /**
     * The host name verifier for HTTPS connections
     */
    private final HostnameVerifier hostnameVerifier;
    /**
     * The user agent to use
     */
    private final String userAgent;
    /**
     * URI of the endpoint
     */
    private final String endpoint;
    /**
     * The host part for the Host header
     */
    private final String host;
    /**
     * The cookies for this connection
     */
    private final Map<String, Cookie> cookies;
    /**
     * Login/Password for the endpoint, if any, used for an HTTP Basic authentication
     */
    private final String authToken;

    /**
     * Initializes this connection
     *
     * @param endpoint URI of the endpoint (base target URI)
     */
    public HttpConnection(String endpoint) {
        this(endpoint, null, null);
    }

    /**
     * Initializes this connection
     *
     * @param endpoint URI of the endpoint (base target URI)
     * @param login    Login for the endpoint, if any, used for an HTTP Basic authentication
     * @param password Password for the endpoint, if any, used for an HTTP Basic authentication
     */
    public HttpConnection(String endpoint, String login, String password) {
        SSLContext sc = null;
        try {
            sc = SSLContext.getInstance("TLSv1.2");
            sc.init(null, new TrustManager[]{TRUST_MANAGER_ACCEPT_ALL}, new SecureRandom());
        } catch (NoSuchAlgorithmException | KeyManagementException exception) {
            Logging.get().error(exception);
        }
        this.sslContext = sc;
        this.hostnameVerifier = HOSTNAME_VERIFIER_ACCEPT_ALL;
        this.userAgent = USER_AGENT_DEFAULT;
        this.endpoint = endpoint;
        String[] components = URIUtils.parse(endpoint);
        this.host = components[URIUtils.COMPONENT_AUTHORITY];
        this.cookies = new HashMap<>();
        if (login != null && password != null) {
            String buffer = (login + ":" + password);
            this.authToken = Base64.encodeBase64(buffer);
        } else {
            this.authToken = null;
        }
    }

    @Override
    public void close() {
        // nothing to do, HTTP connections are one-shot
    }

    /**
     * Sends an HTTP request to the endpoint, completed with an URI complement
     *
     * @param uriComplement The URI complement to append to the original endpoint URI, if any
     * @param method        The HTTP method to use, if any
     * @param body          The request body, if any
     * @param contentType   The request body content type, if any
     * @param accept        The MIME type to accept for the response, if any
     * @return The response, or null if the request failed before reaching the server
     */
    public HttpResponse request(String uriComplement, String method, String body, String contentType, String accept) {
        return request(uriComplement, method, body.getBytes(IOUtils.CHARSET), contentType, false, accept);
    }

    /**
     * Sends an HTTP request to the endpoint, completed with an URI complement
     *
     * @param uriComplement The URI complement to append to the original endpoint URI, if any
     * @param method        The HTTP method to use, if any
     * @param accept        The MIME type to accept for the response, if any
     * @return The response, or null if the request failed before reaching the server
     */
    public HttpResponse request(String uriComplement, String method, String accept) {
        return request(uriComplement, method, null, null, false, accept);
    }

    /**
     * Sends an HTTP request to the endpoint, completed with an URI complement
     *
     * @param uriComplement The URI complement to append to the original endpoint URI, if any
     * @param method        The HTTP method to use, if any
     * @param body          The request body, if any
     * @param contentType   The request body content type, if any
     * @param compressed    Whether the body is compressed with gzip
     * @param accept        The MIME type to accept for the response, if any
     * @return The response, or null if the request failed before reaching the server
     */
    public HttpResponse request(String uriComplement, String method, byte[] body, String contentType, boolean compressed, String accept) {
        String uri = (endpoint != null ? endpoint : "") + (uriComplement != null ? uriComplement : "");
        try {
            HttpURLConnection connection = createConnection(uri, method, body, contentType, compressed, accept);
            return doConnect(connection, body);
        } catch (IOException exception) {
            Logging.get().error(exception);
            return null;
        }
    }

    /**
     * Sends an HTTP request to the endpoint, completed with an URI complement
     *
     * @param uriComplement The URI complement to append to the original endpoint URI, if any
     * @param method        The HTTP method to use, if any
     * @param body          The request body, if any
     * @param contentType   The request body content type, if any
     * @param compressed    Whether the body is compressed with gzip
     * @param accept        The MIME type to accept for the response, if any
     * @param headers       The additional HTTP headers if any
     * @return The response, or null if the request failed before reaching the server
     */
    public HttpResponse request(String uriComplement, String method, byte[] body, String contentType, boolean compressed, String accept, Couple... headers) {
        String uri = (endpoint != null ? endpoint : "") + (uriComplement != null ? uriComplement : "");
        try {
            HttpURLConnection connection = createConnection(uri, method, body, contentType, compressed, accept);
            if (headers != null) {
                for (int i = 0; i != headers.length; i++) {
                    if (headers[i] != null) {
                        connection.setRequestProperty(headers[i].x.toString(), headers[i].y.toString());
                    }
                }
            }
            return doConnect(connection, body);
        } catch (IOException exception) {
            Logging.get().error(exception);
            return null;
        }
    }

    /**
     * Creates the connection
     *
     * @param uri         The target URI to connect to
     * @param method      The HTTP method to use, if any
     * @param body        The request body, if any
     * @param contentType The request body content type, if any
     * @param compressed  Whether the body is compressed with gzip
     * @param accept      The MIME type to accept for the response, if any
     * @return The created connection,or null if there was an error
     * @throws IOException When the connection cannot be opened
     */
    private HttpURLConnection createConnection(String uri, String method, byte[] body, String contentType, boolean compressed, String accept) throws IOException {
        URL url = new URL(uri);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        if (connection instanceof HttpsURLConnection) {
            // for SSL connections we should do this
            ((HttpsURLConnection) connection).setHostnameVerifier(hostnameVerifier);
            ((HttpsURLConnection) connection).setSSLSocketFactory(sslContext.getSocketFactory());
        }
        connection.setInstanceFollowRedirects(false);
        connection.setRequestMethod(method == null || method.isEmpty() ? HttpConstants.METHOD_GET : method);

        // Host header
        if (host != null)
            connection.setRequestProperty(HttpConstants.HEADER_HOST, host);
        else {
            String[] components = URIUtils.parse(uri);
            String host = components[URIUtils.COMPONENT_AUTHORITY];
            if (host != null)
                connection.setRequestProperty(HttpConstants.HEADER_HOST, host);
        }
        connection.setRequestProperty(HttpConstants.HEADER_USER_AGENT, userAgent);

        // Authorization header
        if (authToken != null)
            connection.setRequestProperty(HttpConstants.HEADER_AUTHORIZATION, "Basic " + authToken);

        // Cookie header
        if (!cookies.isEmpty()) {
            StringBuilder builder = new StringBuilder();
            boolean first = true;
            for (Cookie cookie : cookies.values()) {
                if (!first)
                    builder.append("; ");
                first = false;
                builder.append(cookie.name);
                builder.append("=");
                builder.append(cookie.value);
            }
            connection.setRequestProperty(HttpConstants.HEADER_COOKIE, builder.toString());
        }

        // Accept header
        if (accept != null)
            connection.setRequestProperty(HttpConstants.HEADER_ACCEPT, accept);

        // Content-* headers
        if (body != null) {
            if (contentType != null)
                connection.setRequestProperty(HttpConstants.HEADER_CONTENT_TYPE, contentType);
            if (compressed)
                connection.setRequestProperty(HttpConstants.HEADER_CONTENT_ENCODING, "gzip");
            connection.setRequestProperty(HttpConstants.HEADER_CONTENT_LENGTH, Integer.toString(body.length));
        }

        connection.setUseCaches(false);
        connection.setDoOutput(true);
        return connection;
    }

    /**
     * Makes the HTTP connection
     *
     * @param connection The connection
     * @param body       The request body, if any
     * @return The response, or null if the request failed before reaching the server
     */
    private HttpResponse doConnect(HttpURLConnection connection, byte[] body) {
        if (body != null) {
            try (OutputStream stream = connection.getOutputStream()) {
                stream.write(body);
            } catch (IOException exception) {
                Logging.get().error(exception);
                return null;
            }
        }

        int code;
        try {
            code = connection.getResponseCode();
        } catch (IOException exception) {
            Logging.get().error(exception);
            connection.disconnect();
            return null;
        }

        String responseContentType = connection.getContentType();
        byte[] responseBody = null;
        if (connection.getContentLengthLong() == -1 || connection.getContentLengthLong() > 0) {
            // if the content length is unknown or if there is content
            // for codes 4xx and 5xx, use the error stream
            // otherwise use the input stream
            try (InputStream is = ((code >= 400 && code < 600) ? connection.getErrorStream() : connection.getInputStream())) {
                responseBody = IOUtils.load(is);
            } catch (IOException exception) {
                Logging.get().error(exception);
            }
        }
        connection.disconnect();

        HttpResponse response = new HttpResponse(code, responseContentType, responseBody);
        for (Map.Entry<String, List<String>> header : connection.getHeaderFields().entrySet()) {
            if (header.getValue() == null)
                continue;
            if (HttpConstants.HEADER_SET_COOKIE.equalsIgnoreCase(header.getKey())) {
                for (String value : header.getValue()) {
                    Cookie cookie = new Cookie(value);
                    cookies.put(cookie.name, cookie);
                }
            } else {
                for (String value : header.getValue()) {
                    response.addHeader(header.getKey(), value);
                }
            }
        }
        return response;
    }

    /**
     * Represents a cookie for a connection
     */
    private final class Cookie {
        /**
         * The name of the cookie
         */
        public final String name;
        /**
         * The associated value
         */
        public final String value;
        /**
         * The properties associated to the cookie
         */
        public final Collection<String> properties;

        /**
         * Initializes this cookie
         *
         * @param content The original content
         */
        public Cookie(String content) {
            String data = content.trim();
            int indexEqual = content.indexOf('=');
            int indexSemicolon = content.indexOf(';');
            this.name = content.substring(0, indexEqual).trim();
            this.value = content.substring(indexEqual + 1, indexSemicolon < 0 ? content.length() : indexSemicolon).trim();
            this.properties = new ArrayList<>();
            if (indexSemicolon > 0) {
                data = data.substring(indexSemicolon + 1).trim();
                while (!data.isEmpty()) {
                    indexSemicolon = data.indexOf(';');
                    String value = data.substring(0, indexSemicolon < 0 ? data.length() : indexSemicolon).trim();
                    properties.add(value);
                    if (indexSemicolon < 0)
                        break;
                    data = data.substring(indexSemicolon + 1).trim();
                }
            }
        }
    }
}
