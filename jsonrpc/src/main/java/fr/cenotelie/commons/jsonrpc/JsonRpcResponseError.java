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

/**
 * Represents an error response in the Json-Rpc protocol
 *
 * @author Laurent Wouters
 */
public class JsonRpcResponseError implements JsonRpcResponse {
    /**
     * Creates an error for parsing errors
     *
     * @param identifier The identifier of the request
     * @return The error
     */
    public static JsonRpcResponseError newParseError(String identifier) {
        return new JsonRpcResponseError(identifier, -32700, "Parse error", null);
    }

    /**
     * Creates an error for an invalid request
     *
     * @param identifier The identifier of the request
     * @return The error
     */
    public static JsonRpcResponseError newInvalidRequest(String identifier) {
        return new JsonRpcResponseError(identifier, -32600, "Invalid Request", null);
    }

    /**
     * Creates an error when the method is not found
     *
     * @param identifier The identifier of the request
     * @return The error
     */
    public static JsonRpcResponseError newMethodNotFound(String identifier) {
        return new JsonRpcResponseError(identifier, -32601, "Method not found", null);
    }

    /**
     * Creates an error for invalid parameters
     *
     * @param identifier The identifier of the request
     * @return The error
     */
    public static JsonRpcResponseError newInvalidParameters(String identifier) {
        return new JsonRpcResponseError(identifier, -32602, "Invalid parameters", null);
    }

    /**
     * Creates an error for an internal error
     *
     * @param identifier The identifier of the request
     * @return The error
     */
    public static JsonRpcResponseError newInternalError(String identifier) {
        return new JsonRpcResponseError(identifier, -32603, "Invalid Error", null);
    }

    /**
     * Creates an error for an un-initialized server error
     *
     * @param identifier The identifier of the request
     * @return The error
     */
    public static JsonRpcResponseError newServerNotInitialized(String identifier) {
        return new JsonRpcResponseError(identifier, -32002, "Server not initialized", null);
    }

    /**
     * Creates an error for an unknown error
     *
     * @param identifier The identifier of the request
     * @return The error
     */
    public static JsonRpcResponseError newUnknownError(String identifier) {
        return new JsonRpcResponseError(identifier, -32001, "Server not initialized", null);
    }

    /**
     * The identifier for the corresponding request
     */
    private final String identifier;
    /**
     * The error's code
     */
    private final int code;
    /**
     * The error's message
     */
    private final String message;
    /**
     * The error's additional data
     */
    private final Object data;

    /**
     * Initializes this response
     *
     * @param identifier The identifier for the corresponding request
     * @param code       The error's code
     * @param message    The error's message
     * @param data       The error's additional data
     */
    public JsonRpcResponseError(String identifier, int code, String message, Object data) {
        this.identifier = identifier;
        this.code = code;
        this.message = message;
        this.data = data;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public boolean isError() {
        return true;
    }

    /**
     * Gets the error's code
     *
     * @return The error's code
     */
    public int getCode() {
        return code;
    }

    /**
     * Gets the error's message
     *
     * @return The error's message
     */
    public String getMessage() {
        return message;
    }

    /**
     * Gets the error's additional data
     *
     * @return The error's additional data
     */
    public Object getData() {
        return data;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"jsonrpc\": \"2.0\", \"id\": ");
        if (identifier == null)
            builder.append("null");
        else {
            builder.append("\"");
            builder.append(TextUtils.escapeStringJSON(identifier));
            builder.append("\"");
        }
        builder.append(", \"error\": {\"code\": ");
        builder.append(Integer.toString(code));
        builder.append(", \"message\": \"");
        builder.append(TextUtils.escapeStringJSON(message));
        builder.append("\"");
        if (data != null) {
            builder.append(", \"data\": ");
            Json.serialize(builder, data);
        }
        builder.append("}}");
        return builder.toString();
    }
}
