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
 * Registration options for the text save events
 *
 * @author Laurent Wouters
 */
public class TextDocumentSaveRegistrationOptions extends TextDocumentRegistrationOptions {
    /**
     * The client is supposed to include the content on save
     */
    private final boolean includeText;

    /**
     * Initializes this structure
     *
     * @param documentSelector A document selector to identify the scope of the registration
     * @param includeText      The client is supposed to include the content on save
     */
    public TextDocumentSaveRegistrationOptions(DocumentSelector documentSelector, boolean includeText) {
        super(documentSelector);
        this.includeText = includeText;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public TextDocumentSaveRegistrationOptions(ASTNode definition) {
        super(definition);
        boolean includeText = false;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            if ("includeText".equals(name)) {
                if (nodeValue.getSymbol().getID() == JsonLexer.ID.LITERAL_TRUE)
                    includeText = true;
            }
        }
        this.includeText = includeText;
    }

    /**
     * Gets whether the client is supposed to include the content on save
     *
     * @return Whether the client is supposed to include the content on save
     */
    public boolean getIncludeText() {
        return includeText;
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"documentSelector\": ");
        if (documentSelector == null)
            builder.append("null");
        else
            builder.append(documentSelector.serializedJSON());
        builder.append(", \"includeText\": ");
        builder.append(includeText);
        builder.append("}");
        return builder.toString();
    }
}

