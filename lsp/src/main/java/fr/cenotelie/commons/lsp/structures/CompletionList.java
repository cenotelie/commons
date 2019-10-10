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
import fr.cenotelie.commons.utils.json.JsonLexer;
import fr.cenotelie.commons.utils.json.JsonParser;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * Represents a collection of CompletionItem to be presented in the editor.
 *
 * @author Laurent Wouters
 */
public class CompletionList implements Serializable {
    /**
     * Whether this list is not complete
     * Further typing should result in recomputing this list.
     */
    private final boolean isIncomplete;
    /**
     * The completion items
     */
    private final CompletionItem[] items;

    /**
     * Gets whether this list is not complete
     *
     * @return Whether this list is not complete
     */
    public boolean isIncomplete() {
        return isIncomplete;
    }

    /**
     * Gets the completion items
     *
     * @return The completion items
     */
    public CompletionItem[] getItems() {
        return items;
    }

    /**
     * Initializes this structure
     *
     * @param isIncomplete Whether this list is not complete
     * @param items        The completion items
     */
    public CompletionList(boolean isIncomplete, CompletionItem[] items) {
        this.isIncomplete = isIncomplete;
        this.items = items;
    }

    /**
     * Initializes this structure
     *
     * @param definition   The serialized definition
     * @param deserializer The deserializer to use
     */
    public CompletionList(ASTNode definition, JsonDeserializer deserializer) {
        boolean isIncomplete = false;
        CompletionItem[] items = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "isIncomplete": {
                    if (nodeValue.getSymbol().getID() == JsonLexer.ID.LITERAL_TRUE)
                        isIncomplete = true;
                    else if (nodeValue.getSymbol().getID() == JsonLexer.ID.LITERAL_FALSE)
                        isIncomplete = false;
                    else {
                        String value = TextUtils.unescape(nodeValue.getValue());
                        value = value.substring(1, value.length() - 1);
                        isIncomplete = value.equalsIgnoreCase("true");
                    }
                    break;
                }
                case "items": {
                    if (nodeValue.getSymbol().getID() == JsonParser.ID.array) {
                        items = new CompletionItem[nodeValue.getChildren().size()];
                        int index = 0;
                        for (ASTNode nodeItem : nodeValue.getChildren()) {
                            items[index++] = new CompletionItem(nodeItem, deserializer);
                        }
                    }
                    break;
                }
            }
        }
        this.isIncomplete = isIncomplete;
        this.items = items != null ? items : new CompletionItem[0];
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"isIncomplete\": ");
        builder.append(isIncomplete);
        builder.append(", \"items\": [");
        for (int i = 0; i != items.length; i++) {
            if (i == 0)
                builder.append(", ");
            builder.append(items[i].serializedJSON());
        }
        builder.append("]}");
        return builder.toString();
    }
}
