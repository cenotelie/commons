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
 * Parameters for the notification of a document being opened on the client
 *
 * @author Laurent Wouters
 */
public class DidOpenTextDocumentParams implements Serializable {
    /**
     * The document that was opened.
     */
    private final TextDocumentItem textDocument;

    /**
     * Gets the document that was opened
     *
     * @return The document that was opened
     */
    public TextDocumentItem getTextDocument() {
        return textDocument;
    }

    /**
     * Initializes this structure
     *
     * @param textDocument The document that was opened
     */
    public DidOpenTextDocumentParams(TextDocumentItem textDocument) {
        this.textDocument = textDocument;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public DidOpenTextDocumentParams(ASTNode definition) {
        TextDocumentItem textDocument = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "textDocument": {
                    textDocument = new TextDocumentItem(nodeValue);
                    break;
                }
            }
        }
        this.textDocument = textDocument;
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
