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
import fr.cenotelie.hime.redist.ASTNode;

/**
 * Represents a reference to a command.
 * Provides a title which will be used to represent a command in the UI.
 * Commands are identified using a string identifier and the protocol currently doesn't specify a set of well known commands.
 * So executing a command requires some tool extension code.
 *
 * @author Laurent Wouters
 */
public class Command implements Serializable {
    /**
     * Title of the command, like `save`.
     */
    private final String title;
    /**
     * The identifier of the actual command handler.
     */
    private final String command;
    /**
     * Arguments that the command handler should be invoked with.
     */
    private final Object[] arguments;

    /**
     * Gets the title of the command, like `save`.
     *
     * @return Title of the command, like `save`.
     */
    public String getTitle() {
        return title;
    }

    /**
     * Gets the identifier of the actual command handler.
     *
     * @return The identifier of the actual command handler.
     */
    public String getCommand() {
        return command;
    }

    /**
     * Gets the arguments that the command handler should be invoked with.
     *
     * @return Arguments that the command handler should be invoked with.
     */
    public Object[] getArguments() {
        return arguments;
    }

    /**
     * Initializes this structure
     *
     * @param title     Title of the command, like `save`.
     * @param command   The identifier of the actual command handler.
     * @param arguments Arguments that the command handler should be invoked with.
     */
    public Command(String title, String command, Object[] arguments) {
        this.title = title;
        this.command = command;
        this.arguments = arguments;
    }

    /**
     * Initializes this structure
     *
     * @param definition   The serialized definition
     * @param deserializer The current deserializer
     */
    public Command(ASTNode definition, JsonDeserializer deserializer) {
        String title = "";
        String command = "";
        Object[] arguments = null;
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
                case "command": {
                    command = TextUtils.unescape(nodeValue.getValue());
                    command = command.substring(1, command.length() - 1);
                    break;
                }
                case "arguments": {
                    arguments = new Object[nodeValue.getChildren().size()];
                    int index = 0;
                    for (ASTNode arg : nodeValue.getChildren()) {
                        arguments[index++] = deserializer.deserialize(arg, command);
                    }
                    break;
                }
            }
        }
        this.title = title;
        this.command = command;
        this.arguments = arguments;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"title\": \"");
        builder.append(TextUtils.escapeStringJSON(title));
        builder.append("\", \"command\": \"");
        builder.append(TextUtils.escapeStringJSON(command));
        builder.append("\"");
        if (arguments != null) {
            builder.append(", \"arguments\": [");
            for (int i = 0; i != arguments.length; i++) {
                if (i != 0)
                    builder.append(", ");
                Json.serialize(builder, arguments[i]);
            }
            builder.append("]");
        }
        builder.append("}");
        return builder.toString();
    }
}
