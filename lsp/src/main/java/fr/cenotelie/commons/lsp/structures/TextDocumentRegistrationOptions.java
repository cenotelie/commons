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
import fr.cenotelie.commons.utils.json.JsonParser;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * Basic registration options that can be extended
 *
 * @author Laurent Wouters
 */
public class TextDocumentRegistrationOptions implements Serializable {
    /**
     * A document selector to identify the scope of the registration.
     * If set to null the document selector provided on the client side will be used.
     */
    protected final DocumentSelector documentSelector;

    /**
     * Gets the document selector to identify the scope of the registration
     *
     * @return The document selector to identify the scope of the registration
     */
    public DocumentSelector getDocumentSelector() {
        return documentSelector;
    }

    /**
     * Initializes this structure
     *
     * @param documentSelector A document selector to identify the scope of the registration
     */
    public TextDocumentRegistrationOptions(DocumentSelector documentSelector) {
        this.documentSelector = documentSelector;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public TextDocumentRegistrationOptions(ASTNode definition) {
        DocumentSelector documentSelector = null;

        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "documentSelector": {
                    if (nodeValue.getSymbol().getID() == JsonParser.ID.array)
                        documentSelector = new DocumentSelector(nodeValue);
                    break;
                }
            }
        }
        this.documentSelector = documentSelector;
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
        builder.append("}");
        return builder.toString();
    }
}
