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
import fr.cenotelie.commons.utils.json.Json;
import fr.cenotelie.commons.utils.json.JsonLexer;
import fr.cenotelie.commons.utils.json.JsonParser;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * The result of a hover request
 *
 * @author Laurent Wouters
 */
public class Hover implements Serializable {
    /**
     * The hover's content
     */
    private final Object contents;
    /**
     * An optional range inside a text document that is used to visualize a hover, e.g. by changing the background color.
     */
    private final Range range;

    /**
     * Gets the hover's content
     *
     * @return The hover's content
     */
    public Object getContents() {
        return contents;
    }

    /**
     * Gets the range inside a text document that is used to visualize a hover
     *
     * @return The range inside a text document that is used to visualize a hover
     */
    public Range getRange() {
        return range;
    }

    /**
     * Initializes this structure
     *
     * @param content The hover's content
     */
    public Hover(MarkupContent content) {
        this(content, null);
    }

    /**
     * Initializes this structure
     *
     * @param content The hover's content
     * @param range   The range inside a text document that is used to visualize a hover
     */
    public Hover(MarkupContent content, Range range) {
        this.contents = content;
        this.range = range;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public Hover(ASTNode definition) {
        Object contents = null;
        Range range = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "contents": {
                    if (nodeValue.getSymbol().getID() == JsonParser.ID.array) {
                        Object[] array = new Object[nodeValue.getChildren().size()];
                        int index = 0;
                        for (ASTNode nodeItem : nodeValue.getChildren())
                            array[index++] = loadMarkedString(nodeItem);
                        contents = array;
                    } else if (nodeValue.getSymbol().getID() == JsonLexer.ID.LITERAL_STRING) {
                        contents = new MarkedStringMarkdown(nodeValue);
                    } else if (nodeValue.getSymbol().getID() == JsonParser.ID.object) {
                        if (nodeValue.getChildren().isEmpty())
                            break;
                        nodeMemberName = nodeValue.getChildren().get(0).getChildren().get(0);
                        name = TextUtils.unescape(nodeMemberName.getValue());
                        name = name.substring(1, name.length() - 1);
                        switch (name) {
                            case "kind":
                                contents = new MarkupContent(nodeValue);
                                break;
                            case "language":
                                contents = new MarkedStringCodeBlock(nodeValue);
                                break;
                        }
                    }
                    break;
                }
                case "range": {
                    range = new Range(nodeValue);
                    break;
                }
            }
        }
        this.contents = contents;
        this.range = range;
    }

    /**
     * Loads a marked string from a serialized definition
     *
     * @param definition The serialized definition
     * @return The marked string
     */
    private static MarkedString loadMarkedString(ASTNode definition) {
        switch (definition.getSymbol().getID()) {
            case JsonLexer.ID.LITERAL_STRING:
                return new MarkedStringMarkdown(definition);
            case JsonParser.ID.object:
                return new MarkedStringCodeBlock(definition);
        }
        return null;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"contents\": ");
        Json.serialize(contents);
        if (range != null) {
            builder.append(", \"range\": ");
            builder.append(range.serializedJSON());
        }
        builder.append("}");
        return builder.toString();
    }
}
