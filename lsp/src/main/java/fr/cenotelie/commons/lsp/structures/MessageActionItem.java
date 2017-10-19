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
 * An item for a request to show a message for action
 *
 * @author Laurent Wouters
 */
public class MessageActionItem implements Serializable {
    /**
     * A short title like 'Retry', 'Open Log' etc.
     */
    private final String title;

    /**
     * Gets the title
     *
     * @return The title
     */
    public String getTitle() {
        return title;
    }

    /**
     * Initializes this structure
     *
     * @param title The document's URI
     */
    public MessageActionItem(String title) {
        this.title = title;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public MessageActionItem(ASTNode definition) {
        String title = "";
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "title": {
                    title = TextUtils.unescape(nodeValue.getValue());
                    title = title.substring(1, title.length() - 1);
                    break;
                }
            }
        }
        this.title = title;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"title\": \"" +
                TextUtils.escapeStringJSON(title) +
                "\"}";
    }
}
