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

import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * The parameter for a request to show a message and take action
 *
 * @author Laurent Wouters
 */
public class ShowMessageRequestParams extends ShowMessageParams {
    /**
     * The message action items to present
     */
    private final MessageActionItem[] actions;

    /**
     * Gets the message action items to present
     *
     * @return The message action items to present
     */
    public MessageActionItem[] getActions() {
        return actions;
    }

    /**
     * Initializes this structure
     *
     * @param type    The message type
     * @param message The actual message
     * @param actions The message action items to present
     */
    public ShowMessageRequestParams(int type, String message, MessageActionItem[] actions) {
        super(type, message);
        this.actions = actions;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public ShowMessageRequestParams(ASTNode definition) {
        super(definition);
        MessageActionItem[] actions = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "actions": {
                    actions = new MessageActionItem[nodeValue.getChildren().size()];
                    int index = 0;
                    for (ASTNode action : nodeValue.getChildren()) {
                        actions[index++] = new MessageActionItem(action);
                    }
                }
            }
        }
        this.actions = actions;
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"type\": ");
        builder.append(Integer.toString(getType()));
        builder.append(", \"message\": \"");
        builder.append(TextUtils.escapeStringJSON(getMessage()));
        builder.append("\"");
        if (actions != null) {
            builder.append(", \"actions\": [");
            for (int i = 0; i != actions.length; i++) {
                if (i != 0)
                    builder.append(", ");
                builder.append(actions[0].serializedJSON());
            }
            builder.append("]");
        }
        builder.append("}");
        return builder.toString();
    }
}
