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

import fr.cenotelie.commons.utils.IOUtils;
import fr.cenotelie.commons.utils.collections.Couple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * A response to an HTTP request
 *
 * @author Laurent Wouters
 */
public class HttpResponse {
    /**
     * The HTTP response code
     */
    private final int code;
    /**
     * The content type for the response body, if any
     */
    private final String contentType;
    /**
     * The response body, if any
     */
    private byte[] bodyBytes;
    /**
     * The response body as a string
     */
    private String bodyString;
    /**
     * The response's headers
     */
    private Collection<Couple<String, String>> headers;

    /**
     * Initializes this response
     *
     * @param code The response code
     */
    public HttpResponse(int code) {
        this.code = code;
        this.contentType = null;
        this.bodyBytes = null;
    }

    /**
     * Initializes this response
     *
     * @param code        The response code
     * @param contentType The response content type, if any
     * @param body        The response content, if any
     */
    public HttpResponse(int code, String contentType, byte[] body) {
        this.code = code;
        this.contentType = contentType;
        this.bodyBytes = body;
    }

    /**
     * Initializes this response
     *
     * @param code        The response code
     * @param contentType The response content type, if any
     * @param body        The response content, if any
     */
    public HttpResponse(int code, String contentType, String body) {
        this.code = code;
        this.contentType = contentType;
        this.bodyString = body;
    }

    /**
     * Gets the HTTP response code
     *
     * @return The HTTP response code
     */
    public int getCode() {
        return code;
    }

    /**
     * Gets the content type for the response body, if any
     *
     * @return the content type for the response body, if any
     */
    public String getContentType() {
        return contentType;
    }

    /**
     * Gets the response body, if any, as bytes
     *
     * @return The response body, if any, as bytes
     */
    public byte[] getBodyAsBytes() {
        if (bodyBytes != null)
            return bodyBytes;
        if (bodyString != null) {
            bodyBytes = bodyString.getBytes(IOUtils.CHARSET);
            return bodyBytes;
        }
        return null;
    }

    /**
     * Gets the response body, if any, as a string
     *
     * @return The response body, if any, as a string
     */
    public String getBodyAsString() {
        if (bodyString != null)
            return bodyString;
        if (bodyBytes != null) {
            bodyString = new String(bodyBytes, IOUtils.CHARSET);
            return bodyString;
        }
        return null;
    }

    /**
     * Gets the response's header
     *
     * @return The response's header
     */
    public Collection<Couple<String, String>> getHeaders() {
        if (headers == null)
            return Collections.emptyList();
        return Collections.unmodifiableCollection(headers);
    }

    /**
     * Adds a header for the response
     *
     * @param name  The header's name
     * @param value The header's associated value
     */
    public void addHeader(String name, String value) {
        if (headers == null)
            headers = new ArrayList<>();
        headers.add(new Couple<>(name, value));
    }
}
