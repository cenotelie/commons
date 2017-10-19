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

package fr.cenotelie.commons.lsp.structures;

import fr.cenotelie.commons.utils.json.JsonDeserializer;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * ClientCapabilities now define capabilities for dynamic registration, workspace and text document features the client supports.
 * The experimental can be used to pass experimental capabilities under development.
 * For future compatibility a ClientCapabilities object literal can have more properties set than currently defined.
 * Servers receiving a ClientCapabilities object literal with unknown properties should ignore these properties.
 * A missing property should be interpreted as an absence of the capability.
 * If a property is missing that defines sub properties all sub properties should be interpreted as an absence of the capability.
 * TODO: add constants
 *
 * @author Laurent Wouters
 */
public class ClientCapabilities extends Capabilities {
    /**
     * Initializes this structure
     */
    public ClientCapabilities() {
        super();
    }

    /**
     * Initializes this structure
     *
     * @param definition   The serialized definition
     * @param deserializer The deserializer to use
     */
    public ClientCapabilities(ASTNode definition, JsonDeserializer deserializer) {
        super(definition, deserializer);
    }
}
