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
 * The parameters for the code lens request
 *
 * @author Laurent Wouters
 */
public class CodeLensParams implements Serializable {
    /**
     * The document to request code lens for
     */
    private final TextDocumentIdentifier textDocument;

    /**
     * Gets the document to request code lens for
     *
     * @return The document to request code lens for
     */
    public TextDocumentIdentifier getTextDocument() {
        return textDocument;
    }

    /**
     * Initializes this structure
     *
     * @param textDocument The document to request code lens for
     */
    public CodeLensParams(TextDocumentIdentifier textDocument) {
        this.textDocument = textDocument;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public CodeLensParams(ASTNode definition) {
        TextDocumentIdentifier textDocument = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "textDocument": {
                    textDocument = new TextDocumentIdentifier(nodeValue);
                    break;
                }
            }
        }
        this.textDocument = textDocument != null ? textDocument : new TextDocumentIdentifier("");
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"textDocument\": " +
                textDocument.serializedJSON() +
                "}";
    }
}
