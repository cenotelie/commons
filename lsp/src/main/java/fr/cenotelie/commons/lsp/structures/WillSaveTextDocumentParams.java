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
 * Parameter for the will save document notification
 *
 * @author Laurent Wouters
 */
public class WillSaveTextDocumentParams implements Serializable {
    /**
     * The document that will be saved
     */
    private final TextDocumentIdentifier textDocument;
    /**
     * The reason for saving
     */
    private final int reason;

    /**
     * Initializes this structure
     *
     * @param textDocument The document that will be saved
     * @param reason       The reason for saving
     */
    public WillSaveTextDocumentParams(TextDocumentIdentifier textDocument, int reason) {
        this.textDocument = textDocument;
        this.reason = reason;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public WillSaveTextDocumentParams(ASTNode definition) {
        TextDocumentIdentifier textDocument = null;
        int reason = TextDocumentSaveReason.MANUAL;
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
                case "reason": {
                    reason = Integer.parseInt(nodeValue.getValue());
                    break;
                }
            }
        }
        this.textDocument = textDocument;
        this.reason = reason;
    }

    /**
     * Gets the document that will be saved
     *
     * @return The document that will be saved
     */
    public TextDocumentIdentifier getTextDocument() {
        return textDocument;
    }

    /**
     * Gets the reason for saving
     *
     * @return The reason for saving
     */
    public int getReason() {
        return reason;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"textDocument\": " +
                textDocument.serializedJSON() +
                ", \"reason\": " +
                reason +
                "}";
    }
}
