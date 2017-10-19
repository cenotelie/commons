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

import java.io.Reader;
import java.util.List;

/**
 * Represents a server for a Json-Rpc protocol
 *
 * @author Laurent Wouters
 */
public interface JsonRpcServer {
    /**
     * Handles the specified input
     *
     * @param input The input for the server
     * @return The serialized protocol response
     */
    String handle(String input);

    /**
     * Handles the specified input
     *
     * @param input The input for the server
     * @return The serialized protocol response
     */
    String handle(Reader input);

    /**
     * Handles a request
     *
     * @param request The request
     * @return The response object, or null in the case of a notification
     */
    JsonRpcResponse handle(JsonRpcRequest request);

    /**
     * Handles a batch of requests
     *
     * @param requests The requests
     * @return The response objects
     */
    List<JsonRpcResponse> handle(List<JsonRpcRequest> requests);
}
