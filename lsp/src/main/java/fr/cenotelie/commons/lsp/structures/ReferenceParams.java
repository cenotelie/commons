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

import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * The parameters for the textDocument/references request
 *
 * @author Laurent Wouters
 */
public class ReferenceParams extends TextDocumentPositionParams {
    /**
     * The context for the request
     */
    private final ReferenceContext context;

    /**
     * Initializes this structure
     *
     * @param textDocument The text document
     * @param position     The position inside the text document
     * @param context      The context for the request
     */
    public ReferenceParams(TextDocumentIdentifier textDocument, Position position, ReferenceContext context) {
        super(textDocument, position);
        this.context = context;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public ReferenceParams(ASTNode definition) {
        super(definition);
        ReferenceContext context = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            if ("context".equals(name)) {
                context = new ReferenceContext(nodeValue);
            }
        }
        this.context = context;
    }

    /**
     * Gets the context for the request
     *
     * @return The context for the request
     */
    public ReferenceContext getContext() {
        return context;
    }

    @Override
    public String serializedJSON() {
        return "{\"textDocument\": " +
                textDocument.serializedJSON() +
                ", \"position\": " +
                position.serializedJSON() +
                ", \"context\": " +
                context.serializedJSON() +
                "}";
    }
}
