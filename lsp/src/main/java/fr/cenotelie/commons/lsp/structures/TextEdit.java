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

import java.util.Comparator;

/**
 * A textual edit applicable to a text document.
 *
 * @author Laurent Wouters
 */
public class TextEdit implements Serializable {
    /**
     * The in-order comparator of text edits
     */
    public static final Comparator<TextEdit> COMPARATOR_ORDER = new Comparator<TextEdit>() {
        @Override
        public int compare(TextEdit edit1, TextEdit edit2) {
            return edit1.getRange().getStart().compareTo(edit2.getRange().getStart());
        }
    };

    /**
     * The inverse order comparator of text edits
     */
    public static final Comparator<TextEdit> COMPARATOR_INVERSE = new Comparator<TextEdit>() {
        @Override
        public int compare(TextEdit edit1, TextEdit edit2) {
            return edit2.getRange().getStart().compareTo(edit1.getRange().getStart());
        }
    };

    /**
     * The range of the text document to be manipulated.
     * To insert text into a document create a range where start === end.
     */
    private final Range range;

    /**
     * The string to be inserted. For delete operations use an empty string.
     */
    private final String newText;

    /**
     * Gets the range of the text document to be manipulated.
     *
     * @return The range of the text document to be manipulated.
     */
    public Range getRange() {
        return range;
    }

    /**
     * Gets the string to be inserted.
     *
     * @return The string to be inserted.
     */
    public String getNewText() {
        return newText;
    }

    /**
     * Gets whether this edit is an insert
     *
     * @return Whether this edit is an insert
     */
    public boolean isInsert() {
        return range.isEmpty();
    }

    /**
     * Get whether this edit is a deletion
     *
     * @return Whether this edit is a deletion
     */
    public boolean isDeletion() {
        return (newText == null || newText.isEmpty());
    }

    /**
     * Initializes this structure
     *
     * @param range   The range of the text document to be manipulated.
     * @param newText The string to be inserted.
     */
    public TextEdit(Range range, String newText) {
        this.range = range;
        this.newText = newText;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public TextEdit(ASTNode definition) {
        Range range = null;
        String newText = "";
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "newText": {
                    newText = TextUtils.unescape(nodeValue.getValue());
                    newText = newText.substring(1, newText.length() - 1);
                    break;
                }
                case "character": {
                    range = new Range(nodeValue);
                    break;
                }
            }
        }
        this.newText = newText;
        this.range = range != null ? range : new Range(new Position(0, 0), new Position(0, 0));
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"range\": " +
                range.serializedJSON() +
                ", \"newText\": \"" +
                TextUtils.escapeStringJSON(newText) +
                "\"}";
    }
}
