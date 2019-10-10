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
 * Parameters for the notification of a document that has been saved on the client
 *
 * @author Laurent Wouters
 */
public class DidSaveTextDocumentParams implements Serializable {
    /**
     * The document that was saved
     */
    private final TextDocumentIdentifier textDocument;
    /**
     * Optional the content when saved.
     * Depends on the includeText value when the save notification was requested.
     */
    private final String text;

    /**
     * Initializes this structure
     *
     * @param textDocument The document that was saved
     * @param text         The content when saved
     */
    public DidSaveTextDocumentParams(TextDocumentIdentifier textDocument, String text) {
        this.textDocument = textDocument;
        this.text = text;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public DidSaveTextDocumentParams(ASTNode definition) {
        TextDocumentIdentifier textDocument = null;
        String text = null;
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
                case "text": {
                    text = TextUtils.unescape(nodeValue.getValue());
                    text = text.substring(1, text.length() - 1);
                    break;
                }
            }
        }
        this.textDocument = textDocument;
        this.text = text;
    }

    /**
     * Gets the document that was saved
     *
     * @return The document that was saved
     */
    public TextDocumentIdentifier getTextDocument() {
        return textDocument;
    }

    /**
     * Gets the content when saved
     *
     * @return The content when saved
     */
    public String getText() {
        return text;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"textDocument\": ");
        builder.append(textDocument.serializedJSON());
        if (text != null) {
            builder.append(", \"text\": \"");
            builder.append(TextUtils.escapeStringJSON(text));
            builder.append("\"");
        }
        builder.append("}");
        return builder.toString();
    }
}