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

import fr.cenotelie.commons.utils.api.Reply;
import fr.cenotelie.commons.utils.api.ReplyResult;

/**
 * Implements a Json-Rpc client for testing purposes
 *
 * @author Laurent Wouters
 */
public class TestClient extends JsonRpcClientBase {
    /**
     * The server
     */
    private final JsonRpcServer server;

    /**
     * Initializes this client
     *
     * @param server The server
     */
    public TestClient(JsonRpcServer server) {
        super();
        this.server = server;
    }

    @Override
    public Reply send(String message, JsonRpcContext context) {
        return new ReplyResult<>(server.handle(message));
    }
}
