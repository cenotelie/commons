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
import fr.cenotelie.commons.utils.json.JsonLexer;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * Registration options for the code lens request on a document
 *
 * @author Laurent Wouters
 */
public class CodeLensRegistrationOptions extends TextDocumentRegistrationOptions {
    /**
     * The server provides support to resolve code lenses
     */
    private final boolean resolveProvider;

    /**
     * Gets whether the server provides support to resolve code lenses
     *
     * @return Whether the server provides support to resolve code lenses
     */
    public boolean getResolveProvider() {
        return resolveProvider;
    }

    /**
     * Initializes this structure
     *
     * @param documentSelector A document selector to identify the scope of the registration
     * @param resolveProvider  Whether the server provides support to resolve code lenses
     */
    public CodeLensRegistrationOptions(DocumentSelector documentSelector, boolean resolveProvider) {
        super(documentSelector);
        this.resolveProvider = resolveProvider;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public CodeLensRegistrationOptions(ASTNode definition) {
        super(definition);
        boolean resolveProvider = false;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "resolveProvider": {
                    if (nodeValue.getSymbol().getID() == JsonLexer.ID.LITERAL_TRUE)
                        resolveProvider = true;
                    else if (nodeValue.getSymbol().getID() == JsonLexer.ID.LITERAL_FALSE)
                        resolveProvider = false;
                    else {
                        String value = TextUtils.unescape(nodeValue.getValue());
                        value = value.substring(1, value.length() - 1);
                        resolveProvider = value.equalsIgnoreCase("true");
                    }
                }
            }
        }
        this.resolveProvider = resolveProvider;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"documentSelector\": ");
        if (documentSelector == null)
            builder.append("null");
        else
            builder.append(documentSelector.serializedJSON());
        builder.append(", \"resolveProvider\": ");
        builder.append(resolveProvider);
        builder.append("}");
        return builder.toString();
    }
}
