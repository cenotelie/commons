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
import fr.cenotelie.commons.utils.json.JsonLexer;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * The parameters for an operation cancel request
 *
 * @author Laurent Wouters
 */
public class CancelParams implements Serializable {
    /**
     * The identifier of the operation to cancel
     */
    private final String identifier;

    /**
     * Gets the identifier of the operation to cancel
     *
     * @return The identifier of the operation to cancel
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * Initializes this structure
     *
     * @param identifier The identifier of the operation to cancel
     */
    public CancelParams(String identifier) {
        this.identifier = identifier;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public CancelParams(ASTNode definition) {
        String identifier = "";
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "id": {
                    switch (nodeValue.getSymbol().getID()) {
                        case JsonLexer.ID.LITERAL_INTEGER:
                            identifier = definition.getValue();
                            break;
                        case JsonLexer.ID.LITERAL_DECIMAL:
                            identifier = definition.getValue();
                            break;
                        case JsonLexer.ID.LITERAL_DOUBLE:
                            identifier = definition.getValue();
                            break;
                        case JsonLexer.ID.LITERAL_STRING:
                            identifier = TextUtils.unescape(definition.getValue());
                            identifier = identifier.substring(1, identifier.length() - 1);
                            break;
                    }
                    break;
                }
            }
        }
        this.identifier = identifier;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"id\": \"" +
                TextUtils.escapeStringJSON(identifier) +
                "\"}";
    }
}
