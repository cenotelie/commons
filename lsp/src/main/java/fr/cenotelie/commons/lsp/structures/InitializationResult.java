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

import fr.cenotelie.commons.utils.Serializable;
import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.commons.utils.json.JsonDeserializer;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * The result of a server initialization
 *
 * @author Laurent Wouters
 */
public class InitializationResult implements Serializable {
    /**
     * The capabilities the language server provides.
     */
    private final ServerCapabilities capabilities;

    /**
     * Gets the capabilities the language server provides.
     *
     * @return The capabilities the language server provides.
     */
    public ServerCapabilities getCapabilities() {
        return capabilities;
    }

    /**
     * Initializes this structure
     *
     * @param capabilities The capabilities the language server provides.
     */
    public InitializationResult(ServerCapabilities capabilities) {
        this.capabilities = capabilities;
    }

    /**
     * Initializes this structure
     *
     * @param definition   The serialized definition
     * @param deserializer The current deserializer
     */
    public InitializationResult(ASTNode definition, JsonDeserializer deserializer) {
        ServerCapabilities capabilities = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "capabilities": {
                    capabilities = new ServerCapabilities(nodeValue, deserializer);
                    break;
                }
            }
        }
        this.capabilities = capabilities != null ? capabilities : new ServerCapabilities();
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"capabilities\": " +
                capabilities.serializedJSON() +
                "}";
    }
}
