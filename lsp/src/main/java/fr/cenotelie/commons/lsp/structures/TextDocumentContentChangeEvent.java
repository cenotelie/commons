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
 * An event describing a change to a text document.
 * If range and rangeLength are omitted the new text is considered to be the full content of the document.
 *
 * @author Laurent Wouters
 */
public class TextDocumentContentChangeEvent implements Serializable {
    /**
     * The in-order comparator of events
     */
    public static final Comparator<TextDocumentContentChangeEvent> COMPARATOR_ORDER = (event1, event2) -> {
        if (event1.range == null)
            return -1;
        if (event2.range == null)
            return 1;
        return event1.range.getStart().compareTo(event2.getRange().getStart());
    };

    /**
     * The inverse order comparator of events
     */
    public static final Comparator<TextDocumentContentChangeEvent> COMPARATOR_INVERSE = (event1, event2) -> {
        if (event2.range == null)
            return -1;
        if (event1.range == null)
            return 1;
        return event2.range.getStart().compareTo(event1.getRange().getStart());
    };


    /**
     * The length of the range when omitted
     */
    public static final int NO_RANGE = -1;

    /**
     * The range of the document that changed
     */
    private final Range range;
    /**
     * The length of the range that got replaced (-1 when omitted)
     */
    private final int rangeLength;
    /**
     * The new text of the range/document
     */
    private final String text;

    /**
     * Initializes this structure
     *
     * @param range       The range of the document that changed
     * @param rangeLength The length of the range that got replaced (-1 when omitted)
     * @param text        The new text of the range/document
     */
    public TextDocumentContentChangeEvent(Range range, int rangeLength, String text) {
        this.range = range;
        this.rangeLength = rangeLength;
        this.text = text;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public TextDocumentContentChangeEvent(ASTNode definition) {
        Range range = null;
        int rangeLength = NO_RANGE;
        String text = "";
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
                case "rangeLength": {
                    rangeLength = Integer.parseInt(nodeValue.getValue());
                    break;
                }
                case "text": {
                    text = TextUtils.unescape(nodeValue.getValue());
                    text = text.substring(1, text.length() - 1);
                    break;
                }
            }
        }
        this.range = range;
        this.rangeLength = rangeLength;
        this.text = text;
    }

    /**
     * Gets the range of the document that changed
     *
     * @return The range of the document that changed
     */
    public Range getRange() {
        return range;
    }

    /**
     * Gets the length of the range that got replaced (-1 when omitted)
     *
     * @return The length of the range that got replaced (-1 when omitted)
     */
    public int getRangeLength() {
        return rangeLength;
    }

    /**
     * Gets the new text of the range/document
     *
     * @return The new text of the range/document
     */
    public String getText() {
        return text;
    }

    /**
     * Gets whether this event is a full replacement of the text
     *
     * @return Whether this event is a full replacement of the text
     */
    public boolean isFullReplace() {
        return (range == null && rangeLength == NO_RANGE);
    }

    /**
     * Returns the equivalent edit, if possible
     *
     * @return The equivalent edit , if possible, null otherwise
     */
    public TextEdit toEdit() {
        if (range == null || rangeLength < 0)
            return null;
        return new TextEdit(range, text);
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        boolean first = true;
        if (range != null) {
            builder.append("\"range\": ");
            builder.append(range.serializedJSON());
            first = false;
        }
        if (rangeLength >= 0) {
            if (!first)
                builder.append(", ");
            builder.append("\"rangeLength\": ");
            builder.append(rangeLength);
            first = false;
        }
        if (!first)
            builder.append(", ");
        builder.append("\"text\": \"");
        builder.append(TextUtils.escapeStringJSON(text));
        builder.append("\"}");
        return builder.toString();
    }
}
