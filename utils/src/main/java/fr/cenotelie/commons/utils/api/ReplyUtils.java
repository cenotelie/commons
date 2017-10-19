/*******************************************************************************
 * Copyright (c) 2017 Association Cénotélie (cenotelie.fr)
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

package fr.cenotelie.commons.utils.api;

import fr.cenotelie.commons.utils.Serializable;
import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.commons.utils.http.HttpConstants;
import fr.cenotelie.commons.utils.http.HttpResponse;
import fr.cenotelie.commons.utils.json.Json;
import fr.cenotelie.commons.utils.json.JsonParser;
import fr.cenotelie.commons.utils.logging.BufferedLogger;
import fr.cenotelie.hime.redist.ASTNode;

import java.net.HttpURLConnection;
import java.util.Collection;

/**
 * Utility APIs for the management of API replies
 *
 * @author Laurent Wouters
 */
public class ReplyUtils {
    /**
     * Translates an API reply to an HTTP response
     *
     * @param reply The reply
     * @return The HTTP response
     */
    public static HttpResponse toHttpResponse(Reply reply) {
        // replies mapped to HTTP error codes
        if (reply == null)
            // client got banned
            return new HttpResponse(HttpURLConnection.HTTP_FORBIDDEN);
        if (reply instanceof ReplyUnauthenticated)
            return new HttpResponse(HttpURLConnection.HTTP_UNAUTHORIZED);
        if (reply instanceof ReplyUnauthorized)
            return new HttpResponse(HttpURLConnection.HTTP_FORBIDDEN);
        if (reply instanceof ReplyExpiredSession)
            return new HttpResponse(HttpConstants.HTTP_SESSION_EXPIRED);
        if (reply instanceof ReplyUnsupported)
            return new HttpResponse(HttpURLConnection.HTTP_NOT_IMPLEMENTED);
        if (reply instanceof ReplyNotFound)
            return new HttpResponse(HttpURLConnection.HTTP_NOT_FOUND);
        if (reply instanceof ReplyApiError)
            return new HttpResponse(HttpURLConnection.HTTP_BAD_REQUEST, HttpConstants.MIME_JSON, ((ReplyApiError) reply).getError().serializedJSON(((ReplyApiError) reply).getSupplementaryMessage()));
        if (reply instanceof ReplyException)
            return new HttpResponse(HttpURLConnection.HTTP_INTERNAL_ERROR, HttpConstants.MIME_TEXT_PLAIN, ReplyException.MESSAGE);

        // other failures (including ReplyFailure) mapped to HTTP_UNKNOWN_ERROR
        if (!reply.isSuccess())
            return new HttpResponse(HttpConstants.HTTP_UNKNOWN_ERROR, HttpConstants.MIME_TEXT_PLAIN, reply.getMessage());

        // handle a single result
        if (reply instanceof ReplyResult) {
            Object data = ((ReplyResult) reply).getData();
            if (data == null)
                return new HttpResponse(HttpURLConnection.HTTP_OK);
            if (data instanceof Serializable)
                return new HttpResponse(HttpURLConnection.HTTP_OK, HttpConstants.MIME_JSON, ((Serializable) data).serializedJSON());
            else
                return new HttpResponse(HttpURLConnection.HTTP_OK, HttpConstants.MIME_TEXT_PLAIN, data.toString());
        }

        // handle a collection of results
        if (reply instanceof ReplyResultCollection) {
            Collection data = ((ReplyResultCollection) reply).getData();
            StringBuilder builder = new StringBuilder("[");
            boolean first = true;
            for (Object obj : data) {
                if (!first)
                    builder.append(", ");
                first = false;
                Json.serialize(builder, obj);
            }
            builder.append("]");
            return new HttpResponse(HttpURLConnection.HTTP_OK, HttpConstants.MIME_JSON, builder.toString());
        }

        // general case, only OK
        return new HttpResponse(HttpURLConnection.HTTP_OK);
    }

    /**
     * Translates an HTTP response to an reply
     *
     * @param response     The response
     * @param deserializer the deserializer to use
     * @return The reply
     */
    public static Reply fromHttpResponse(HttpResponse response, ApiDeserializer deserializer) {
        // replies mapped to HTTP error codes
        if (response == null)
            return ReplyNetworkError.instance();
        // handle special HTTP codes
        if (response.getCode() == HttpURLConnection.HTTP_UNAUTHORIZED)
            return ReplyUnauthenticated.instance();
        if (response.getCode() == HttpURLConnection.HTTP_FORBIDDEN)
            return ReplyUnauthorized.instance();
        if (response.getCode() == HttpConstants.HTTP_SESSION_EXPIRED)
            return ReplyExpiredSession.instance();
        if (response.getCode() == HttpURLConnection.HTTP_NOT_IMPLEMENTED)
            return ReplyUnsupported.instance();
        if (response.getCode() == HttpURLConnection.HTTP_NOT_FOUND)
            return ReplyNotFound.instance();
        if (response.getCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
            BufferedLogger bufferedLogger = new BufferedLogger();
            ASTNode root = Json.parse(bufferedLogger, response.getBodyAsString());
            if (root == null)
                return new ReplyFailure(response.getBodyAsString());
            return new ReplyApiError(ReplyApiError.parseApiError(root), ReplyApiError.parseSupplementary(root));
        }
        if (response.getCode() == HttpURLConnection.HTTP_INTERNAL_ERROR)
            return new ReplyException(null); // exception not preserved

        // other failures (including ReplyFailure) mapped to HTTP_UNKNOWN_ERROR
        if (response.getCode() == HttpConstants.HTTP_UNKNOWN_ERROR)
            return new ReplyFailure(response.getBodyAsString());
        // handle other failures
        if (response.getCode() != HttpURLConnection.HTTP_OK)
            return new ReplyFailure(response.getBodyAsString() != null ? response.getBodyAsString() : "failure (HTTP " + response.getCode() + ")");

        String contentType = response.getContentType();
        if (contentType != null && !contentType.isEmpty()) {
            // we've got a content type
            int index = contentType.indexOf(";");
            if (index > 0)
                contentType = contentType.substring(0, index).trim();
            switch (contentType) {
                case HttpConstants.MIME_JSON:
                    return fromHttpResponseJSON(response, deserializer);
                case HttpConstants.MIME_OCTET_STREAM:
                    return fromHttpResponseBinary(response);
                default:
                    // other text
                    return fromHttpResponseOther(response);
            }
        } else {
            // no content type
            if (response.getBodyAsString() != null && !response.getBodyAsString().isEmpty()) {
                // too bad we have content ...
                // assume plain text
                return new ReplyResult<>(response.getBodyAsString());
            } else {
                return ReplySuccess.instance();
            }
        }
    }

    /**
     * Translates an HTTP response to an reply when the content type is JSON
     *
     * @param response     The response
     * @param deserializer The deserializer to use
     * @return The reply
     */
    public static Reply fromHttpResponseJSON(HttpResponse response, ApiDeserializer deserializer) {
        String content = response.getBodyAsString();
        if (content == null || content.isEmpty())
            return ReplySuccess.instance();
        BufferedLogger bufferedLogger = new BufferedLogger();
        ASTNode root = Json.parse(bufferedLogger, response.getBodyAsString());
        if (root == null)
            return new ReplyFailure(bufferedLogger.getErrorsAsString());
        Object data = deserializer.deserialize(root, null);
        if (data instanceof Collection)
            return new ReplyResultCollection<>((Collection) data);
        return new ReplyResult<>(data);
    }

    /**
     * Translates an HTTP response to an reply when the content is a binary stream
     *
     * @param response The response
     * @return The reply
     */
    public static Reply fromHttpResponseBinary(HttpResponse response) {
        byte[] content = response.getBodyAsBytes();
        if (content == null || content.length == 0)
            return ReplySuccess.instance();
        return new ReplyResult<>(content);
    }

    /**
     * Translates an HTTP response to an reply for other content types
     *
     * @param response The response
     * @return The reply
     */
    public static Reply fromHttpResponseOther(HttpResponse response) {
        String content = response.getBodyAsString();
        if (content == null || content.isEmpty())
            return ReplySuccess.instance();
        return new ReplyResult<>(content);
    }

    /**
     * Parses a reply serialized in JSON
     *
     * @param content      The content
     * @param deserializer The deserializer to use
     * @return The result
     */
    public static Reply parse(String content, ApiDeserializer deserializer) {
        BufferedLogger bufferedLogger = new BufferedLogger();
        ASTNode root = Json.parse(bufferedLogger, content);
        if (root == null)
            return new ReplyFailure(bufferedLogger.getErrorsAsString());
        if (root.getSymbol().getID() == JsonParser.ID.array)
            return new ReplyFailure("Unexpected JSON format");
        return parse(root, deserializer);
    }

    /**
     * Parses a result serialized in JSON
     *
     * @param root         The content
     * @param deserializer The deserializer to use
     * @return The result
     */
    public static Reply parse(ASTNode root, ApiDeserializer deserializer) {
        String kind = null;
        boolean isSuccess = false;
        String message = null;
        ASTNode nodePayload = null;
        for (ASTNode memberNode : root.getChildren()) {
            String memberName = TextUtils.unescape(memberNode.getChildren().get(0).getValue());
            memberName = memberName.substring(1, memberName.length() - 1);
            ASTNode memberValue = memberNode.getChildren().get(1);
            switch (memberName) {
                case "kind": {
                    kind = TextUtils.unescape(memberValue.getValue());
                    kind = kind.substring(1, kind.length() - 1);
                    break;
                }
                case "isSuccess": {
                    String value = TextUtils.unescape(memberValue.getValue());
                    isSuccess = Boolean.parseBoolean(value);
                    break;
                }
                case "message": {
                    message = TextUtils.unescape(memberValue.getValue());
                    message = message.substring(1, message.length() - 1);
                    break;
                }
                case "payload":
                    nodePayload = memberValue;
                    break;
            }
        }

        if (kind == null)
            return new ReplyFailure("Unexpected JSON format");
        if (ReplyApiError.class.getCanonicalName().equals(kind))
            return new ReplyApiError(ReplyApiError.parseApiError(nodePayload), message);
        if (ReplyException.class.getCanonicalName().equals(kind))
            return new ReplyException(null); // exception not preserved
        if (ReplyExpiredSession.class.getCanonicalName().equals(kind))
            return ReplyExpiredSession.instance();
        if (ReplyFailure.class.getCanonicalName().equals(kind))
            return new ReplyFailure(message);
        if (ReplyNetworkError.class.getCanonicalName().equals(kind))
            return new ReplyNetworkError(message);
        if (ReplyResult.class.getCanonicalName().equals(kind))
            return new ReplyResult<>(deserializer.deserialize(nodePayload, null));
        if (ReplyResultCollection.class.getCanonicalName().equals(kind))
            return new ReplyResultCollection<>((Collection<?>) deserializer.deserialize(nodePayload, null));
        if (ReplySuccess.class.getCanonicalName().equals(kind))
            return new ReplySuccess(message);
        if (ReplyUnauthenticated.class.getCanonicalName().equals(kind))
            return ReplyUnauthenticated.instance();
        if (ReplyUnauthorized.class.getCanonicalName().equals(kind))
            return ReplyUnauthorized.instance();
        if (ReplyUnsupported.class.getCanonicalName().equals(kind))
            return ReplyUnsupported.instance();

        if (isSuccess)
            return new ReplySuccess(message);
        else
            return new ReplyFailure(message);
    }
}
