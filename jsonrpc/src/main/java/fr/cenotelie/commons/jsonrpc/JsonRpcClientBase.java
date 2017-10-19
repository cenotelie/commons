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

package fr.cenotelie.commons.jsonrpc;

import fr.cenotelie.commons.utils.api.*;
import fr.cenotelie.hime.redist.ASTNode;
import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.commons.utils.json.Json;
import fr.cenotelie.commons.utils.json.JsonDeserializer;
import fr.cenotelie.commons.utils.json.JsonLexer;
import fr.cenotelie.commons.utils.json.JsonParser;
import fr.cenotelie.commons.utils.logging.BufferedLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A base implementation of a Json-Rpc client
 *
 * @author Laurent Wouters
 */
public abstract class JsonRpcClientBase implements JsonRpcClient {
    /**
     * The counter for the unique identifiers of requests
     */
    protected final AtomicInteger counter;
    /**
     * The de-serializer to use
     */
    protected final JsonDeserializer deserializer;

    /**
     * Initializes this client
     */
    public JsonRpcClientBase() {
        this(JsonDeserializer.DEFAULT);
    }

    /**
     * Initializes this client
     *
     * @param deserializer The de-serializer to use for responses
     */
    public JsonRpcClientBase(JsonDeserializer deserializer) {
        this.counter = new AtomicInteger(1);
        this.deserializer = deserializer;
    }

    @Override
    public int getNextId() {
        return counter.getAndIncrement();
    }

    @Override
    public Reply send(JsonRpcRequest request) {
        return sendAndDeserialize(
                request.serializedJSON(),
                new JsonRpcContextSingle(request.isNotification() ? null : request.getMethod())
        );
    }

    @Override
    public Reply send(List<JsonRpcRequest> requests) {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        boolean first = true;
        for (JsonRpcRequest request : requests) {
            if (!first)
                builder.append(", ");
            first = false;
            builder.append(request.serializedJSON());
        }
        builder.append("]");
        return sendAndDeserialize(builder.toString(), new JsonRpcContextBatch(requests));
    }

    /**
     * Send a serialized message to the server and get the de-serialized response
     *
     * @param message The message to sendAndDeserialize
     * @param context The de-serialization context
     * @return The reply
     */
    protected Reply sendAndDeserialize(String message, JsonRpcContext context) {
        Reply reply = send(message, context);
        if (!reply.isSuccess())
            return reply;
        if (context.isEmpty())
            return ReplySuccess.instance();
        return deserializeResponses(((ReplyResult<String>) reply).getData(), context);
    }

    /**
     * Parses the content of a response
     *
     * @param content The content of a response
     * @param context The de-serialization context
     * @return The reply
     */
    protected Reply deserializeResponses(String content, JsonRpcContext context) {
        if (content == null || content.isEmpty())
            return ReplySuccess.instance();

        BufferedLogger logger = new BufferedLogger();
        ASTNode definition = Json.parse(logger, content);
        if (definition == null || !logger.getErrorMessages().isEmpty())
            return new ReplyApiError(ERROR_RESPONSE_PARSING, logger.getErrorsAsString());
        return deserializeResponses(definition, context);
    }

    /**
     * De-serializes the response objects
     *
     * @param definition The serialized definition
     * @param context    The de-serialization context
     * @return The reply
     */
    protected Reply deserializeResponses(ASTNode definition, JsonRpcContext context) {
        if (definition.getSymbol().getID() == JsonParser.ID.object)
            return deserializeResponse(definition, context);
        if (definition.getSymbol().getID() != JsonParser.ID.array)
            return new ReplyApiError(ERROR_INVALID_RESPONSE, "Response is neither an object nor an array");

        // here, expect an array of responses
        List<JsonRpcResponse> responses = new ArrayList<>();
        for (ASTNode child : definition.getChildren()) {
            Reply reply = deserializeResponse(child, context);
            if (!reply.isSuccess())
                return reply;
            responses.add(((ReplyResult<JsonRpcResponse>) reply).getData());
        }
        return new ReplyResultCollection<>(responses);
    }

    /**
     * De-serializes the response object
     *
     * @param definition The serialized definition
     * @param context    The de-serialization context
     * @return The reply
     */
    protected Reply deserializeResponse(ASTNode definition, JsonRpcContext context) {
        if (definition.getSymbol().getID() != JsonParser.ID.object)
            return new ReplyApiError(ERROR_INVALID_RESPONSE, "Response item in a batch of responses is not an object");

        String jsonRpc = null;
        String identifier = null;
        ASTNode nodeResult = null;
        ASTNode nodeError = null;

        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "jsonrpc": {
                    if (nodeValue.getSymbol().getID() != JsonLexer.ID.LITERAL_STRING)
                        return new ReplyApiError(ERROR_INVALID_RESPONSE);
                    jsonRpc = TextUtils.unescape(nodeValue.getValue());
                    jsonRpc = jsonRpc.substring(1, jsonRpc.length() - 1);
                    break;
                }
                case "id": {
                    switch (nodeValue.getSymbol().getID()) {
                        case JsonLexer.ID.LITERAL_NULL:
                            identifier = null;
                            break;
                        case JsonLexer.ID.LITERAL_FALSE:
                            return null;
                        case JsonLexer.ID.LITERAL_TRUE:
                            return null;
                        case JsonLexer.ID.LITERAL_INTEGER:
                            identifier = nodeValue.getValue();
                            break;
                        case JsonLexer.ID.LITERAL_DECIMAL:
                            identifier = nodeValue.getValue();
                            break;
                        case JsonLexer.ID.LITERAL_DOUBLE:
                            identifier = nodeValue.getValue();
                            break;
                        case JsonLexer.ID.LITERAL_STRING:
                            identifier = TextUtils.unescape(nodeValue.getValue());
                            identifier = identifier.substring(1, identifier.length() - 1);
                            break;
                        default:
                            return null;
                    }
                    break;
                }
                case "result": {
                    nodeResult = nodeValue;
                    break;
                }
                case "error": {
                    nodeError = nodeValue;
                    break;
                }
            }
        }

        if (!Objects.equals(jsonRpc, "2.0"))
            return new ReplyApiError(ERROR_INVALID_RESPONSE, "Not a Json-Rpc 2.0 response");
        if (nodeResult == null && nodeError == null)
            return new ReplyApiError(ERROR_INVALID_RESPONSE, "No result or error in response");
        if (nodeError != null)
            return deserializeResponseError(nodeError, identifier, context);

        Object result;
        if (context == null)
            return new ReplyApiError(ERROR_MISSING_CONTEXT);
        result = deserializer.deserialize(nodeResult, context.getMethodFor(identifier));
        if (result == null)
            return new ReplyApiError(ERROR_INVALID_RESPONSE, "Failed to de-serialize the result in the response");
        return new ReplyResult<>(new JsonRpcResponseResult<>(identifier, result));
    }

    /**
     * De-serializes the error response object
     *
     * @param definition The serialized definition
     * @param identifier The identifier of the current response
     * @param context    The de-serialization context
     * @return The reply
     */
    protected Reply deserializeResponseError(ASTNode definition, String identifier, JsonRpcContext context) {
        if (definition.getSymbol().getID() != JsonParser.ID.object)
            return new ReplyApiError(ERROR_INVALID_RESPONSE, "Error component in Json-Rpc error response is not object");

        int code = 0;
        String message = null;
        Object data = null;

        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "code": {
                    code = Integer.parseInt(nodeValue.getValue());
                    break;
                }
                case "message": {
                    message = TextUtils.unescape(nodeValue.getValue());
                    message = message.substring(1, message.length() - 1);
                    break;
                }
                case "data": {
                    data = deserializer.deserialize(nodeValue, context.getMethodFor(identifier));
                    break;
                }
            }
        }

        if (code == 0 || message == null)
            return new ReplyApiError(ERROR_INVALID_RESPONSE, "Missing code or message in Json-Rpc error response");
        return new ReplyResult<>(new JsonRpcResponseError(identifier, code, message, data));
    }
}
