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
 * Parameters for the show message notification
 *
 * @author Laurent Wouters
 */
public class ShowMessageParams implements Serializable {
    /**
     * The message type
     */
    private final int type;
    /**
     * The actual message
     */
    private final String message;

    /**
     * Initializes this structure
     *
     * @param type    The message type
     * @param message The actual message
     */
    public ShowMessageParams(int type, String message) {
        this.type = type;
        this.message = message;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public ShowMessageParams(ASTNode definition) {
        int type = MessageType.TYPE_LOG;
        String message = "";
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "type": {
                    type = Integer.parseInt(nodeValue.getValue());
                    break;
                }
                case "message": {
                    message = TextUtils.unescape(nodeValue.getValue());
                    message = message.substring(1, message.length() - 1);
                    break;
                }
            }
        }
        this.type = type;
        this.message = message;
    }

    /**
     * Gets the message type
     *
     * @return The message type
     */
    public int getType() {
        return type;
    }

    /**
     * Gets the actual message
     *
     * @return The actual message
     */
    public String getMessage() {
        return message;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"type\": " +
                type +
                ",\"message\": \"" +
                TextUtils.escapeStringJSON(message) +
                "\"}";
    }
}
