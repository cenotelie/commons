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
 * A code lens represents a command that should be shown along with source text, like the number of references, a way to run tests, etc.
 * A code lens is _unresolved_ when no command is associated to it.
 * For performance reasons the creation of a code lens and resolving should be done in two stages.
 *
 * @author Laurent Wouters
 */
public class CodeLens implements Serializable {
    /**
     * The range in which this code lens is valid. Should only span a single line
     */
    private final Range range;
    /**
     * The command this code lens represents
     */
    private final Command command;
    /**
     * The data entry field that is preserved on a code lens item between a code lens and a code lens resolve request
     */
    private Object data;

    /**
     * Gets the range in which this code lens is valid. Should only span a single line
     *
     * @return The range in which this code lens is valid. Should only span a single line
     */
    public Range getRange() {
        return range;
    }

    /**
     * Gets the command this code lens represents
     *
     * @return The command this code lens represents
     */
    public Command getCommand() {
        return command;
    }

    /**
     * Gets the data entry field that is preserved on a code lens item between a code lens and a code lens resolve request
     *
     * @return The data entry field that is preserved on a code lens item between a code lens and a code lens resolve request
     */
    public Object getData() {
        return data;
    }

    /**
     * Sets the data entry field that is preserved on a code lens item between a code lens and a code lens resolve request
     *
     * @param data The data entry field that is preserved on a code lens item between a code lens and a code lens resolve request
     */
    public void setData(Object data) {
        this.data = data;
    }

    /**
     * Initializes this structure
     *
     * @param range The range in which this code lens is valid. Should only span a single line
     */
    public CodeLens(Range range) {
        this(range, null, null);
    }

    /**
     * Initializes this structure
     *
     * @param range   The range in which this code lens is valid. Should only span a single line
     * @param command The command this code lens represents
     */
    public CodeLens(Range range, Command command) {
        this(range, command, null);
    }

    /**
     * Initializes this structure
     *
     * @param range   The range in which this code lens is valid. Should only span a single line
     * @param command The command this code lens represents
     * @param data    The data entry field that is preserved on a code lens item between a code lens and a code lens resolve request
     */
    public CodeLens(Range range, Command command, Object data) {
        this.range = range;
        this.command = command;
        this.data = data;
    }

    /**
     * Initializes this structure
     *
     * @param definition   The serialized definition
     * @param deserializer The current deserializer
     */
    public CodeLens(ASTNode definition, JsonDeserializer deserializer) {
        Range range = null;
        Command command = null;
        Object data = null;
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
                case "command": {
                    command = new Command(nodeValue, deserializer);
                    break;
                }
                case "data": {
                    data = deserializer.deserialize(nodeValue, this);
                    break;
                }
            }
        }
        this.range = range != null ? range : new Range(new Position(0, 0), new Position(0, 0));
        this.command = command;
        this.data = data;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"range\": ");
        builder.append(range.serializedJSON());
        if (command != null) {
            builder.append(", \"command\": ");
            builder.append(command.serializedJSON());
        }
        if (data != null) {
            builder.append(", \"data\": ");
            Json.serialize(builder, data);
        }
        builder.append("}");
        return builder.toString();
    }
}
