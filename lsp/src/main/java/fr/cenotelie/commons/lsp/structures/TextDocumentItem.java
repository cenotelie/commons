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
 * An item to transfer a text document from the client to the server.
 *
 * @author Laurent Wouters
 */
public class TextDocumentItem implements Serializable {
    /**
     * The text document's URI.
     */
    private final String uri;
    /**
     * The text document's language identifier.
     */
    private final String languageId;
    /**
     * The version number of this document (it will strictly increase after each change, including undo/redo).
     */
    private final int version;
    /**
     * The content of the opened text document.
     */
    private final String text;

    /**
     * Gets the text document's URI.
     *
     * @return The text document's URI.
     */
    public String getUri() {
        return uri;
    }

    /**
     * Gets the text document's language identifier.
     *
     * @return The text document's language identifier.
     */
    public String getLanguageId() {
        return languageId;
    }

    /**
     * Gets the version number of this document
     *
     * @return The version number of this document
     */
    public int getVersion() {
        return version;
    }

    /**
     * Gets the content of the opened text document.
     *
     * @return The content of the opened text document.
     */
    public String getText() {
        return text;
    }

    /**
     * Initializes this structure
     *
     * @param uri        The text document's URI.
     * @param languageId The text document's language identifier.
     * @param version    The version number of this document
     * @param text       The content of the opened text document.
     */
    public TextDocumentItem(String uri, String languageId, int version, String text) {
        this.uri = uri;
        this.languageId = languageId;
        this.version = version;
        this.text = text;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public TextDocumentItem(ASTNode definition) {
        String uri = "";
        String languageId = "";
        int version = 0;
        String text = "";
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "uri": {
                    uri = TextUtils.unescape(nodeValue.getValue());
                    uri = uri.substring(1, uri.length() - 1);
                    break;
                }
                case "languageId": {
                    languageId = TextUtils.unescape(nodeValue.getValue());
                    languageId = languageId.substring(1, languageId.length() - 1);
                    break;
                }
                case "version": {
                    version = Integer.parseInt(nodeValue.getValue());
                    break;
                }
                case "text": {
                    text = TextUtils.unescape(nodeValue.getValue());
                    text = text.substring(1, text.length() - 1);
                    break;
                }
            }
        }
        this.uri = uri;
        this.languageId = languageId;
        this.version = version;
        this.text = text;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"uri\": \"" +
                TextUtils.escapeStringJSON(uri) +
                "\", \"languageId\": \"" +
                TextUtils.escapeStringJSON(languageId) +
                "\", \"version\": " +
                Integer.toString(version) +
                ", \"text\": \"" +
                TextUtils.escapeStringJSON(text) +
                "\"}";
    }
}
