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
import fr.cenotelie.commons.utils.json.JsonParser;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * Execute command registration options.
 *
 * @author Laurent Wouters
 */
public class ExecuteCommandRegistrationOptions implements Serializable {
    /**
     * The commands to be executed on the server
     */
    protected final String[] commands;

    /**
     * Initializes this structure
     *
     * @param commands The commands to be executed on the server
     */
    public ExecuteCommandRegistrationOptions(String[] commands) {
        this.commands = commands;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public ExecuteCommandRegistrationOptions(ASTNode definition) {
        String[] commands = null;

        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            if ("commands".equals(name)) {
                if (nodeValue.getSymbol().getID() == JsonParser.ID.array) {
                    commands = new String[nodeValue.getChildren().size()];
                    int index = 0;
                    for (ASTNode nodeCommand : nodeValue.getChildren()) {
                        String value = TextUtils.unescape(nodeCommand.getValue());
                        value = value.substring(1, value.length() - 1);
                        commands[index++] = value;
                    }
                }
            }
        }
        this.commands = commands == null ? new String[0] : commands;
    }

    /**
     * Gets the commands to be executed on the server
     *
     * @return The commands to be executed on the server
     */
    public String[] getCommands() {
        return commands;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"commands\": [");
        if (commands != null) {
            for (int i = 0; i != commands.length; i++) {
                if (i != 0)
                    builder.append(", ");
                builder.append("\"");
                builder.append(TextUtils.escapeStringJSON(commands[i]));
                builder.append("\"");
            }
        }
        builder.append("]}");
        return builder.toString();
    }
}