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
import fr.cenotelie.commons.utils.json.JsonLexer;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * A document highlight is a range inside a text document which deserves special attention.
 * Usually a document highlight is visualized by changing the background color of its range.
 *
 * @author Laurent Wouters
 */
public class DocumentHighlight implements Serializable {
    /**
     * The range this highlight applies to
     */
    private final Range range;
    /**
     * The highlight kind, default is DocumentHighlightKind.Text
     */
    private final int kind;

    /**
     * Gets the range this highlight applies to
     *
     * @return The range this highlight applies to
     */
    public Range getRange() {
        return range;
    }

    /**
     * Gets the highlight kind, default is DocumentHighlightKind.Text
     *
     * @return The highlight kind, default is DocumentHighlightKind.Text
     */
    public int getKind() {
        return kind;
    }

    /**
     * Initializes this structure
     *
     * @param range The range this highlight applies to
     */
    public DocumentHighlight(Range range) {
        this(range, DocumentHighlightKind.TEXT);
    }

    /**
     * Initializes this structure
     *
     * @param range The range this highlight applies to
     * @param kind  The highlight kind, default is DocumentHighlightKind.Text
     */
    public DocumentHighlight(Range range, int kind) {
        this.range = range;
        this.kind = kind;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public DocumentHighlight(ASTNode definition) {
        Range range = null;
        int kind = DocumentHighlightKind.TEXT;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "range": {
                    range = new Range(nodeValue);
                    break;
                }
                case "kind": {
                    if (nodeValue.getSymbol().getID() == JsonLexer.ID.LITERAL_INTEGER)
                        kind = Integer.parseInt(nodeValue.getValue());
                    break;
                }
            }
        }
        this.range = range != null ? range : new Range(new Position(0, 0), new Position(0, 0));
        this.kind = kind;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"range\": " +
                range.serializedJSON() +
                ", \"kind\": " +
                Integer.toString(kind) +
                "}";
    }
}
