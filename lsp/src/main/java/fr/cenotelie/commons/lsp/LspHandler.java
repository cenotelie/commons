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

package fr.cenotelie.commons.lsp;

import fr.cenotelie.commons.jsonrpc.JsonRpcServer;
import fr.cenotelie.commons.utils.json.JsonDeserializer;

/**
 * Represents a handler of LSP requests
 *
 * @author Laurent Wouters
 */
public interface LspHandler extends JsonRpcServer {
    /**
     * Gets the deserializer used for requests
     *
     * @return The deserializer used for requests
     */
    JsonDeserializer getRequestsDeserializer();
}
