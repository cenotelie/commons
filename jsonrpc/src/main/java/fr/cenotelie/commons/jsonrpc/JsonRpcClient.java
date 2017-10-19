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

import fr.cenotelie.commons.utils.api.ApiError;
import fr.cenotelie.commons.utils.api.Reply;

import java.util.List;

/**
 * Represents a client for Json-Rpc protocol
 *
 * @author Laurent Wouters
 */
public interface JsonRpcClient {
    /**
     * Error when the parsing of the incoming response failed
     */
    ApiError ERROR_RESPONSE_PARSING = new ApiError(0x0001,
            "Error while parsing the response",
            "http://cenotelie.fr/xowl/support/jsonrpc/errors/0x0001.html");
    /**
     * Error when the response object is not valid
     */
    ApiError ERROR_INVALID_RESPONSE = new ApiError(0x0002,
            "Invalid response",
            "http://cenotelie.fr/xowl/support/jsonrpc/errors/0x0002.html");
    /**
     * Error when a context is missing while de-serializing
     */
    ApiError ERROR_MISSING_CONTEXT = new ApiError(0x0003,
            "Missing context while de-serializing",
            "http://cenotelie.fr/xowl/support/jsonrpc/errors/0x0003.html");

    /**
     * Gets the next unique identifier for a request
     *
     * @return The next unique identifier
     */
    int getNextId();

    /**
     * Sends serialized data to the server
     *
     * @param message The message to send to the server
     * @param context The current context
     * @return The reply
     */
    Reply send(String message, JsonRpcContext context);

    /**
     * Sends a request to the server
     *
     * @param request The request
     * @return The reply
     */
    Reply send(JsonRpcRequest request);

    /**
     * Sends a batch of requests to the server
     *
     * @param requests The requests
     * @return The reply
     */
    Reply send(List<JsonRpcRequest> requests);
}
