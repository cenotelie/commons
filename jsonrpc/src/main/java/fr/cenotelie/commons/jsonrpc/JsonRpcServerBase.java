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

import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.commons.utils.json.Json;
import fr.cenotelie.commons.utils.json.JsonDeserializer;
import fr.cenotelie.commons.utils.json.JsonLexer;
import fr.cenotelie.commons.utils.json.JsonParser;
import fr.cenotelie.commons.utils.logging.BufferedLogger;
import fr.cenotelie.hime.redist.ASTNode;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A base implementation of a Json-Rpc server
 *
 * @author Laurent Wouters
 */
public abstract class JsonRpcServerBase implements JsonRpcServer {
    /**
     * The de-serializer to use
     */
    protected final JsonDeserializer deserializer;

    /**
     * Initializes this server
     */
    public JsonRpcServerBase() {
        this(JsonDeserializer.DEFAULT);
    }

    /**
     * Initializes this server
     *
     * @param deserializer The de-serializer to use for requests
     */
    public JsonRpcServerBase(JsonDeserializer deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public String handle(String input) {
        return handle(new StringReader(input));
    }

    @Override
    public String handle(Reader input) {
        BufferedLogger logger = new BufferedLogger();
        ASTNode definition = Json.parse(logger, input);
        if (definition == null || !logger.getErrorMessages().isEmpty())
            return JsonRpcResponseError.newParseError(null).serializedJSON();
        return handle(definition);
    }

    /**
     * Handles the specified input
     *
     * @param definition the definition of the request
     * @return The response
     */
    protected String handle(ASTNode definition) {
        Object object = deserializeRequests(definition);
        if (object == null)
            return JsonRpcResponseError.newInvalidRequest(null).serializedJSON();
        if (object instanceof JsonRpcRequest) {
            JsonRpcResponse response = handle((JsonRpcRequest) object);
            if (response == null)
                return null;
            return response.serializedJSON();
        } else {
            List<JsonRpcRequest> requests = ((List<JsonRpcRequest>) object);
            if (requests.isEmpty())
                return JsonRpcResponseError.newInvalidRequest(null).serializedJSON();
            List<JsonRpcResponse> responses = handle(requests);
            if (responses.isEmpty())
                return null;
            return Json.serialize(responses);
        }
    }

    @Override
    public abstract JsonRpcResponse handle(JsonRpcRequest request);

    @Override
    public List<JsonRpcResponse> handle(List<JsonRpcRequest> requests) {
        List<JsonRpcResponse> responses = new ArrayList<>(requests.size());
        for (JsonRpcRequest request : requests) {
            if (request == null)
                responses.add(JsonRpcResponseError.newInvalidRequest(null));
            else {
                JsonRpcResponse response = handle(request);
                if (response != null)
                    responses.add(response);
            }
        }
        return responses;
    }

    /**
     * De-serializes the requests objects
     *
     * @param definition The serialized definition
     * @return The request(s)
     */
    protected Object deserializeRequests(ASTNode definition) {
        if (definition.getSymbol().getID() == JsonParser.ID.object)
            return deserializeRequest(definition);
        if (definition.getSymbol().getID() != JsonParser.ID.array)
            return null;
        List<JsonRpcRequest> requests = new ArrayList<>();
        for (ASTNode child : definition.getChildren()) {
            JsonRpcRequest request = deserializeRequest(child);
            requests.add(request);
        }
        return requests;
    }

    /**
     * De-serializes the request object
     *
     * @param definition The serialized definition
     * @return The request
     */
    protected JsonRpcRequest deserializeRequest(ASTNode definition) {
        if (definition.getSymbol().getID() != JsonParser.ID.object)
            return null;

        String jsonRpc = null;
        String identifier = null;
        String method = null;
        ASTNode params = null;

        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "jsonrpc": {
                    if (nodeValue.getSymbol().getID() != JsonLexer.ID.LITERAL_STRING)
                        return null;
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
                case "method": {
                    if (nodeValue.getSymbol().getID() != JsonLexer.ID.LITERAL_STRING)
                        return null;
                    method = TextUtils.unescape(nodeValue.getValue());
                    method = method.substring(1, method.length() - 1);
                    break;
                }
                case "params": {
                    params = nodeValue;
                    break;
                }
            }
        }

        if (!Objects.equals(jsonRpc, "2.0"))
            return null;
        if (method == null)
            return null;
        Object params2 = params != null ? deserializer.deserialize(params, method) : null;
        return new JsonRpcRequest(identifier, method, params2);
    }
}
