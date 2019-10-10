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
import fr.cenotelie.hime.redist.ASTNode;

/**
 * The context for the textDocument/references request
 *
 * @author Laurent Wouters
 */
public class ReferenceContext implements Serializable {
    /**
     * Include the declaration of the current symbol
     */
    private final boolean includeDeclaration;

    /**
     * Initializes this structure
     *
     * @param includeDeclaration Whether to include the declaration of the current symbol
     */
    public ReferenceContext(boolean includeDeclaration) {
        this.includeDeclaration = includeDeclaration;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public ReferenceContext(ASTNode definition) {
        boolean includeDeclaration = false;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            if ("includeDeclaration".equals(name)) {
                includeDeclaration = Boolean.parseBoolean(nodeValue.getValue());
            }
        }
        this.includeDeclaration = includeDeclaration;
    }

    /**
     * Gets whether to include the declaration of the current symbol
     *
     * @return Whether to include the declaration of the current symbol
     */
    public boolean includeDeclaration() {
        return includeDeclaration;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"includeDeclaration\": " +
                includeDeclaration +
                "}";
    }
}
