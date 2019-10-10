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
import fr.cenotelie.commons.utils.json.JsonDeserializer;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * The parameters for the document on-type formatting command
 *
 * @author Laurent Wouters
 */
public class DocumentOnTypeFormattingParams implements Serializable {
    /**
     * The document to format
     */
    private final TextDocumentIdentifier textDocument;
    /**
     * The position at which this request was sent
     */
    private final Position position;
    /**
     * The character that has been typed
     */
    private final String character;
    /**
     * The format options
     */
    private final FormattingOptions options;

    /**
     * Initializes this structure
     *
     * @param textDocument The document to format
     * @param position     The position at which this request was sent
     * @param character    The character that has been typed
     * @param options      The format options
     */
    public DocumentOnTypeFormattingParams(TextDocumentIdentifier textDocument, Position position, String character, FormattingOptions options) {
        this.textDocument = textDocument;
        this.position = position;
        this.character = character;
        this.options = options;
    }

    /**
     * Initializes this structure
     *
     * @param definition   The serialized definition
     * @param deserializer The current deserializer
     */
    public DocumentOnTypeFormattingParams(ASTNode definition, JsonDeserializer deserializer) {
        TextDocumentIdentifier textDocument = null;
        Position position = null;
        String character = "";
        FormattingOptions options = null;
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
                case "ch": {
                    character = TextUtils.unescape(nodeValue.getValue());
                    character = character.substring(1, character.length() - 1);
                    break;
                }
                case "options": {
                    options = new FormattingOptions(nodeValue, deserializer);
                    break;
                }
            }
        }
        this.textDocument = textDocument != null ? textDocument : new TextDocumentIdentifier("");
        this.position = position != null ? position : new Position(0, 0);
        this.character = character;
        this.options = options != null ? options : new FormattingOptions(4, false);
    }

    /**
     * Gets the document to format
     *
     * @return The document to format
     */
    public TextDocumentIdentifier getTextDocument() {
        return textDocument;
    }

    /**
     * Gets the position at which this request was sent
     *
     * @return The position at which this request was sent
     */
    public Position getPosition() {
        return position;
    }

    /**
     * Gets the character that has been typed
     *
     * @return The character that has been typed
     */
    public String getCharacter() {
        return character;
    }

    /**
     * Gets the format options
     *
     * @return The format options
     */
    public FormattingOptions getOptions() {
        return options;
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
                ", \"ch\": \"" +
                TextUtils.escapeStringJSON(character) +
                "\", \"options\": " +
                options.serializedJSON() +
                "}";
    }
}
