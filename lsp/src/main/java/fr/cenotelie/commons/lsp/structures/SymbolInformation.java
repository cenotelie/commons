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
 * Represents information about programming constructs like variables, classes, interfaces etc.
 *
 * @author Laurent Wouters
 */
public class SymbolInformation implements Serializable {
    /**
     * The name of this symbol
     */
    private final String name;
    /**
     * The kind of this symbol
     */
    private final int kind;
    /**
     * The location of this symbol
     */
    private final Location location;
    /**
     * The name of the symbol containing this symbol
     */
    private final String containerName;

    /**
     * Initializes this structure
     *
     * @param name          The name of this symbol
     * @param kind          The kind of this symbol
     * @param location      The location of this symbol
     * @param containerName The name of the symbol containing this symbol
     */
    public SymbolInformation(String name, int kind, Location location, String containerName) {
        this.name = name;
        this.kind = kind;
        this.location = location;
        this.containerName = containerName;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public SymbolInformation(ASTNode definition) {
        String name = null;
        int kind = SymbolKind.FILE;
        Location location = null;
        String containerName = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String memberName = TextUtils.unescape(nodeMemberName.getValue());
            memberName = memberName.substring(1, memberName.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (memberName) {
                case "name": {
                    name = TextUtils.unescape(nodeValue.getValue());
                    name = name.substring(1, name.length() - 1);
                    break;
                }
                case "kind": {
                    kind = Integer.parseInt(nodeValue.getValue());
                    break;
                }
                case "location": {
                    location = new Location(nodeValue);
                    break;
                }
                case "containerName": {
                    containerName = TextUtils.unescape(nodeValue.getValue());
                    containerName = containerName.substring(1, containerName.length() - 1);
                    break;
                }
            }
        }
        this.name = name;
        this.kind = kind;
        this.location = location;
        this.containerName = containerName;
    }

    /**
     * Gets the name of this symbol
     *
     * @return The name of this symbol
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the kind of this symbol
     *
     * @return The kind of this symbol
     */
    public int getKind() {
        return kind;
    }

    /**
     * Gets the location of this symbol
     *
     * @return The location of this symbol
     */
    public Location getLocation() {
        return location;
    }

    /**
     * Gets the name of the symbol containing this symbol
     *
     * @return The name of the symbol containing this symbol
     */
    public String getContainerName() {
        return containerName;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"name\": \"");
        builder.append(TextUtils.escapeStringJSON(name));
        builder.append("\", \"kind\": ");
        builder.append(kind);
        builder.append(", \"location\": ");
        builder.append(location.serializedJSON());
        if (containerName != null) {
            builder.append(", \"containerName\": \"");
            builder.append(TextUtils.escapeStringJSON(containerName));
            builder.append("\"");
        }
        builder.append("}");
        return builder.toString();
    }
}
