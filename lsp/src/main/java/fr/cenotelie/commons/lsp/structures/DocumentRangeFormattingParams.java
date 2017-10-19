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
 * The parameters for the document range formatting command
 *
 * @author Laurent Wouters
 */
public class DocumentRangeFormattingParams implements Serializable {
    /**
     * The document to format
     */
    private final TextDocumentIdentifier textDocument;
    /**
     * The range to format
     */
    private final Range range;
    /**
     * The format options
     */
    private final FormattingOptions options;

    /**
     * Gets the document to format
     *
     * @return The document to format
     */
    public TextDocumentIdentifier getTextDocument() {
        return textDocument;
    }

    /**
     * Gets the range to format
     *
     * @return The range to format
     */
    public Range getRange() {
        return range;
    }

    /**
     * Gets the format options
     *
     * @return The format options
     */
    public FormattingOptions getOptions() {
        return options;
    }

    /**
     * Initializes this structure
     *
     * @param textDocument The document to format
     * @param range        The range to format
     * @param options      The format options
     */
    public DocumentRangeFormattingParams(TextDocumentIdentifier textDocument, Range range, FormattingOptions options) {
        this.textDocument = textDocument;
        this.range = range;
        this.options = options;
    }

    /**
     * Initializes this structure
     *
     * @param definition   The serialized definition
     * @param deserializer The current deserializer
     */
    public DocumentRangeFormattingParams(ASTNode definition, JsonDeserializer deserializer) {
        TextDocumentIdentifier textDocument = null;
        Range range = null;
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
                case "range": {
                    range = new Range(nodeValue);
                    break;
                }
                case "options": {
                    options = new FormattingOptions(nodeValue, deserializer);
                    break;
                }
            }
        }
        this.textDocument = textDocument != null ? textDocument : new TextDocumentIdentifier("");
        this.range = range != null ? range : new Range(new Position(0, 0), new Position(0, 0));
        this.options = options != null ? options : new FormattingOptions(4, false);
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
                ", \"options\": " +
                options.serializedJSON() +
                "}";
    }
}
