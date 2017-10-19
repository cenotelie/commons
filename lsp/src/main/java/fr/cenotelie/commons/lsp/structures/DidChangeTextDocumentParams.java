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
 * Parameters for the notification of a content change on a document
 *
 * @author Laurent Wouters
 */
public class DidChangeTextDocumentParams implements Serializable {
    /**
     * The document that did change.
     * The version number points to the version after all provided content changes have been applied.
     */
    private final VersionedTextDocumentIdentifier textDocument;
    /**
     * The actual content changes.
     */
    private final TextDocumentContentChangeEvent[] contentChanges;

    /**
     * Gets the document that did change
     *
     * @return The document that did change
     */
    public VersionedTextDocumentIdentifier getTextDocument() {
        return textDocument;
    }

    /**
     * Gets the actual changes
     *
     * @return The actual changes
     */
    public TextDocumentContentChangeEvent[] getContentChanges() {
        return contentChanges;
    }

    /**
     * Initializes this structure
     *
     * @param textDocument   The document that did change
     * @param contentChanges The actual content changes
     */
    public DidChangeTextDocumentParams(VersionedTextDocumentIdentifier textDocument, TextDocumentContentChangeEvent[] contentChanges) {
        this.textDocument = textDocument;
        this.contentChanges = contentChanges;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public DidChangeTextDocumentParams(ASTNode definition) {
        VersionedTextDocumentIdentifier textDocument = null;
        TextDocumentContentChangeEvent[] contentChanges = null;
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
                case "contentChanges": {
                    contentChanges = new TextDocumentContentChangeEvent[nodeValue.getChildren().size()];
                    int index = 0;
                    for (ASTNode change : nodeValue.getChildren())
                        contentChanges[index++] = new TextDocumentContentChangeEvent(change);
                    break;
                }
            }
        }
        this.textDocument = textDocument;
        this.contentChanges = contentChanges;
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
        builder.append(", \"contentChanges\": [");
        for (int i = 0; i != contentChanges.length; i++) {
            if (i != 0)
                builder.append(", ");
            builder.append(contentChanges[i].serializedJSON());
        }
        builder.append("]}");
        return builder.toString();
    }
}
