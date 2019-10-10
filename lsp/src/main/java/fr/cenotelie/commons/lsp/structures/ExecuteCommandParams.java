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
import fr.cenotelie.commons.utils.json.JsonDeserializer;
import fr.cenotelie.commons.utils.json.JsonParser;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * The parameters for the workspace/executeCommand command
 *
 * @author Laurent Wouters
 */
public class ExecuteCommandParams implements Serializable {
    /**
     * The identifier of the actual command handler
     */
    private final String command;
    /**
     * The arguments that the command should be invoked with
     */
    private final Object[] arguments;

    /**
     * Initializes this structure
     *
     * @param command The identifier of the actual command handler
     */
    public ExecuteCommandParams(String command) {
        this.command = command;
        this.arguments = null;
    }

    /**
     * Initializes this structure
     *
     * @param command   The identifier of the actual command handler
     * @param arguments The arguments that the command should be invoked with
     */
    public ExecuteCommandParams(String command, Object... arguments) {
        this.command = command;
        this.arguments = arguments;
    }

    /**
     * Initializes this structure
     *
     * @param definition   The serialized definition
     * @param deserializer The deserializer to use
     */
    public ExecuteCommandParams(ASTNode definition, JsonDeserializer deserializer) {
        String command = null;
        Object[] arguments = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "command": {
                    command = TextUtils.unescape(nodeValue.getValue());
                    command = command.substring(1, command.length() - 1);
                    break;
                }
                case "arguments": {
                    if (nodeValue.getSymbol().getID() == JsonParser.ID.array) {
                        arguments = new String[nodeValue.getChildren().size()];
                        int index = 0;
                        for (ASTNode nodeArg : nodeValue.getChildren())
                            arguments[index++] = deserializer.deserialize(nodeArg, this);
                    }
                    break;
                }
            }
        }
        this.command = command == null ? "" : command;
        this.arguments = arguments;
    }

    /**
     * Gets the identifier of the actual command handler
     *
     * @return The identifier of the actual command handler
     */
    public String getCommand() {
        return command;
    }

    /**
     * Gets the arguments that the command should be invoked with
     *
     * @return The arguments that the command should be invoked with
     */
    public Object[] getArguments() {
        return arguments;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"command\": \"");
        builder.append(TextUtils.escapeStringJSON(command));
        builder.append("\"");
        if (arguments != null) {
            builder.append(", \"arguments\": [");
            for (int i = 0; i != arguments.length; i++) {
                if (i != 0)
                    builder.append(", ");
                builder.append("\"");
                Json.serialize(builder, arguments[i]);
                builder.append("\"");
            }
            builder.append("]}");
        }
        builder.append("}");
        return builder.toString();
    }
}
