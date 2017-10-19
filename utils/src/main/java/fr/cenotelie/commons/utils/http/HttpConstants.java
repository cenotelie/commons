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

/**
 * Constants for using the HTTP protocol
 *
 * @author Laurent Wouters
 */
public interface HttpConstants {
    /**
     * The HTTP method OPTIONS.
     *
     * @see <a href="https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html">Hypertext Transfer Protocol -- HTTP/1.1</a>
     */
    String METHOD_OPTIONS = "OPTIONS";
    /**
     * The HTTP method GET.
     *
     * @see <a href="https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html">Hypertext Transfer Protocol -- HTTP/1.1</a>
     */
    String METHOD_GET = "GET";
    /**
     * The HTTP method POST.
     *
     * @see <a href="https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html">Hypertext Transfer Protocol -- HTTP/1.1</a>
     */
    String METHOD_POST = "POST";
    /**
     * The HTTP method PUT.
     *
     * @see <a href="https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html">Hypertext Transfer Protocol -- HTTP/1.1</a>
     */
    String METHOD_PUT = "PUT";
    /**
     * The HTTP method DELETE.
     *
     * @see <a href="https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html">Hypertext Transfer Protocol -- HTTP/1.1</a>
     */
    String METHOD_DELETE = "DELETE";

    /**
     * The HTTP Origin header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Origin">Origin header on MDN</a>
     */
    String HEADER_ORIGIN = "Origin";
    /**
     * The HTTP Host header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Host">Host header on MDN</a>
     */
    String HEADER_HOST = "Host";
    /**
     * The HTTP User-Agent header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/User-Agent">User-Agent header on MDN</a>
     */
    String HEADER_USER_AGENT = "User-Agent";
    /**
     * The HTTP Authorization header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Authorization">Authorization header on MDN</a>
     */
    String HEADER_AUTHORIZATION = "Authorization";
    /**
     * The HTTP Content-Type header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type">Content-Type header on MDN</a>
     */
    String HEADER_CONTENT_TYPE = "Content-Type";
    /**
     * The HTTP Content-Encoding header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding">Content-Encoding header on MDN</a>
     */
    String HEADER_CONTENT_ENCODING = "Content-Encoding";
    /**
     * The HTTP Content-Length header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Length">Content-Length header on MDN</a>
     */
    String HEADER_CONTENT_LENGTH = "Content-Length";
    /**
     * The HTTP Accept header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Accept">Accept header on MDN</a>
     */
    String HEADER_ACCEPT = "Accept";
    /**
     * The HTTP Cookie header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cookie">Cookie header on MDN</a>
     */
    String HEADER_COOKIE = "Cookie";
    /**
     * The HTTP Set-Cookie header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Set-Cookie">Set-Cookie header on MDN</a>
     */
    String HEADER_SET_COOKIE = "Set-Cookie";
    /**
     * The HTTP Cache-Control header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control">Cache-Control header on MDN</a>
     */
    String HEADER_CACHE_CONTROL = "Cache-Control";
    /**
     * The HTTP X-Frame-Options header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Frame-Options">X-Frame-Options header on MDN</a>
     */
    String HEADER_X_FRAME_OPTIONS = "X-Frame-Options";
    /**
     * The HTTP X-XSS-Protection header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-XSS-Protection">X-XSS-Protection header on MDN</a>
     */
    String HEADER_X_XSS_PROTECTION = "X-XSS-Protection";
    /**
     * The HTTP X-Content-Type-Options header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Content-Type-Options">X-Content-Type-Options header on MDN</a>
     */
    String HEADER_X_CONTENT_TYPE_OPTIONS = "X-Content-Type-Options";
    /**
     * The HTTP Strict-Transport-Security header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Strict-Transport-Security">Strict-Transport-Security header on MDN</a>
     */
    String HEADER_STRICT_TRANSPORT_SECURITY = "Strict-Transport-Security";
    /**
     * The HTTP Access-Control-Allow-Methods header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Access-Control-Allow-Methods">Access-Control-Allow-Methods header on MDN</a>
     */
    String HEADER_ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
    /**
     * The HTTP Access-Control-Allow-Headers header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Access-Control-Allow-Headers">Access-Control-Allow-Headers header on MDN</a>
     */
    String HEADER_ACCESS_CONTROL_ALLOW_HEADERS = "Access-Control-Allow-Headers";
    /**
     * The HTTP Access-Control-Allow-Origin header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Access-Control-Allow-Origin">Access-Control-Allow-Origin header on MDN</a>
     */
    String HEADER_ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
    /**
     * The HTTP Access-Control-Allow-Credentials header.
     *
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Access-Control-Allow-Credentials">Access-Control-Allow-Credentials header on MDN</a>
     */
    String HEADER_ACCESS_CONTROL_ALLOW_CREDENTIALS = "Access-Control-Allow-Credentials";

    /**
     * The MIME type for octet stream
     */
    String MIME_OCTET_STREAM = "binary/octet-stream";
    /**
     * The MIME type for URL encoded content
     */
    String MIME_URLENCODED = "application/x-www-form-urlencoded";
    /**
     * The MIME type for multipart mixed messages
     */
    String MIME_MULTIPART_MIXED = "multipart/mixed";
    /**
     * The MIME type for multipart messages where parts are alternatives
     */
    String MIME_MULTIPART_ALTERNATIVE = "multipart/alternative";
    /**
     * The MIME type for multipart messages with a digest part
     */
    String MIME_MULTIPART_DIGEST = "multipart/digest";
    /**
     * The MIME type for multipart message with parallel parts
     */
    String MIME_MULTIPART_PARALLEL = "multipart/parallel";
    /**
     * The boundary to be used for multipart HTTP requests and responses
     */
    String MULTIPART_BOUNDARY = "xowl_boundary";
    /**
     * The MIME content type for plain text
     */
    String MIME_TEXT_PLAIN = "text/plain";
    /**
     * The MIME content type for JSON
     */
    String MIME_JSON = "application/json";
    /**
     * The MIME type for HTML resources
     */
    String MIME_HTML = "text/html";
    /**
     * The MIME type for RAML resources
     */
    String MIME_RAML = "application/raml+yaml";

    /**
     * HTTP code for an expired user session
     */
    int HTTP_SESSION_EXPIRED = 440;
    /**
     * HTTP code for a SPARQL error
     */
    int HTTP_SPARQL_ERROR = 461;
    /**
     * HTTP code for an unknown error from the server
     */
    int HTTP_UNKNOWN_ERROR = 560;
}
