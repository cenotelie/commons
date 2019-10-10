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
 * The parameters for the symbol rename request from the client to the server
 *
 * @author Laurent Wouters
 */
public class RenameParams implements Serializable {
    /**
     * The document that contains the symbol to be renamed
     */
    private final TextDocumentIdentifier textDocument;
    /**
     * The position of the symbol within the document
     */
    private final Position position;
    /**
     * The new name of the symbol.
     * If the given name is not valid the request must return a ResponseError with an appropriate message set.
     */
    private final String newName;

    /**
     * Initializes this structure
     *
     * @param textDocument The document that contains the symbol to be renamed
     * @param position     The position of the symbol within the document
     * @param newName      The new name of the symbol
     */
    public RenameParams(TextDocumentIdentifier textDocument, Position position, String newName) {
        this.textDocument = textDocument;
        this.position = position;
        this.newName = newName;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public RenameParams(ASTNode definition) {
        TextDocumentIdentifier textDocument = null;
        Position position = null;
        String newName = "";
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
                case "position": {
                    position = new Position(nodeValue);
                    break;
                }
                case "newName": {
                    newName = TextUtils.unescape(nodeValue.getValue());
                    newName = newName.substring(1, newName.length() - 1);
                    break;
                }
            }
        }
        this.textDocument = textDocument != null ? textDocument : new TextDocumentIdentifier("");
        this.position = position != null ? position : new Position(0, 0);
        this.newName = newName;
    }

    /**
     * Gets the document that contains the symbol to be renamed
     *
     * @return The document that contains the symbol to be renamed
     */
    public TextDocumentIdentifier getTextDocument() {
        return textDocument;
    }

    /**
     * Gets the position of the symbol within the document
     *
     * @return The position of the symbol within the document
     */
    public Position getPosition() {
        return position;
    }

    /**
     * Gets the new name of the symbol
     *
     * @return The new name of the symbol
     */
    public String getNewName() {
        return newName;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"textDocument\": " +
                textDocument.serializedJSON() +
                ", \"position\": " +
                position.serializedJSON() +
                ", \"newName\": \"" +
                TextUtils.escapeStringJSON(newName) +
                "\"}";
    }
}
