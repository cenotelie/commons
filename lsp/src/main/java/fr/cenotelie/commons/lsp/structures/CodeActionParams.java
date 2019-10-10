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
 * Parameters for the code action request from the client to the server
 *
 * @author Laurent Wouters
 */
public class CodeActionParams implements Serializable {
    /**
     * The document in which the command was invoked
     */
    private final TextDocumentIdentifier textDocument;
    /**
     * The range for which the command was invoked
     */
    private final Range range;
    /**
     * The context carrying additional information
     */
    private final CodeActionContext context;

    /**
     * Initializes this structure
     *
     * @param textDocument The document in which the command was invoked
     * @param range        The range for which the command was invoked
     * @param context      The context carrying additional information
     */
    public CodeActionParams(TextDocumentIdentifier textDocument, Range range, CodeActionContext context) {
        this.textDocument = textDocument;
        this.range = range;
        this.context = context;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public CodeActionParams(ASTNode definition) {
        TextDocumentIdentifier textDocument = null;
        Range range = null;
        CodeActionContext context = null;
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
                case "range": {
                    range = new Range(nodeValue);
                    break;
                }
                case "context": {
                    context = new CodeActionContext(nodeValue);
                    break;
                }
            }
        }
        this.textDocument = textDocument != null ? textDocument : new TextDocumentIdentifier("");
        this.range = range != null ? range : new Range(new Position(0, 0), new Position(0, 0));
        this.context = context != null ? context : new CodeActionContext(new Diagnostic[0]);
    }

    /**
     * Gets the document in which the command was invoked
     *
     * @return The document in which the command was invoked
     */
    public TextDocumentIdentifier getTextDocument() {
        return textDocument;
    }

    /**
     * Gets the range for which the command was invoked
     *
     * @return The range for which the command was invoked
     */
    public Range getRange() {
        return range;
    }

    /**
     * Gets the context carrying additional information
     *
     * @return The context carrying additional information
     */
    public CodeActionContext getContext() {
        return context;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"textDocument\": " +
                textDocument.serializedJSON() +
                ", \"range\": " +
                range.serializedJSON() +
                ", \"context\": " +
                context.serializedJSON() +
                "}";
    }
}
