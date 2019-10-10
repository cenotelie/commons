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
 * Describes textual changes on a single text document.
 * The text document is referred to as a VersionedTextDocumentIdentifier to allow clients to check the text document version before an edit is applied.
 */
public class TextDocumentEdit implements Serializable {
    /**
     * The text document to change.
     */
    private final VersionedTextDocumentIdentifier textDocument;
    /**
     * The edits to be applied.
     */
    private final TextEdit[] edits;

    /**
     * Initializes this structure
     *
     * @param textDocument The text document to change.
     * @param edits        The edits to be applied.
     */
    public TextDocumentEdit(VersionedTextDocumentIdentifier textDocument, TextEdit[] edits) {
        this.textDocument = textDocument;
        this.edits = edits;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public TextDocumentEdit(ASTNode definition) {
        VersionedTextDocumentIdentifier textDocument = null;
        TextEdit[] edits = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "textDocument": {
                    textDocument = new VersionedTextDocumentIdentifier(nodeValue);
                    break;
                }
                case "edits": {
                    edits = new TextEdit[nodeValue.getChildren().size()];
                    int index = 0;
                    for (ASTNode edit : nodeValue.getChildren())
                        edits[index++] = new TextEdit(edit);
                    break;
                }
            }
        }
        this.textDocument = textDocument != null ? textDocument : new VersionedTextDocumentIdentifier("", 0);
        this.edits = edits != null ? edits : new TextEdit[0];
    }

    /**
     * Gets the text document to change.
     *
     * @return The text document to change.
     */
    public VersionedTextDocumentIdentifier getTextDocument() {
        return textDocument;
    }

    /**
     * Gets the edits to be applied.
     *
     * @return The edits to be applied.
     */
    public TextEdit[] getEdits() {
        return edits;
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
        builder.append(", \"edits\": [");
        for (int i = 0; i != edits.length; i++) {
            if (i != 0)
                builder.append(", ");
            builder.append(edits[i].serializedJSON());
        }
        builder.append("]}");
        return builder.toString();
    }
}
