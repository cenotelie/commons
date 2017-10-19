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
import fr.cenotelie.commons.utils.json.JsonLexer;
import fr.cenotelie.hime.redist.ASTNode;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Value-object describing what options formatting should use
 *
 * @author Laurent Wouters
 */
public class FormattingOptions implements Serializable {
    /**
     * The size of a tab in spaces
     */
    private final int tabSize;
    /**
     * Whether to prefer spaces over tabs
     */
    private final boolean insertSpaces;
    /**
     * The properties for this object
     */
    private final Map<String, Object> properties;

    /**
     * Gets the size of a tab in spaces
     *
     * @return The size of a tab in spaces
     */
    public int getTabSize() {
        return tabSize;
    }

    /**
     * Gets whether to prefer spaces over tabs
     *
     * @return Whether to prefer spaces over tabs
     */
    public boolean isInsertSpaces() {
        return insertSpaces;
    }

    /**
     * Gets the properties of this object
     *
     * @return The properties
     */
    public Collection<String> getProperties() {
        return properties.keySet();
    }

    /**
     * Gets the value associated to a property
     *
     * @param property The property
     * @return The associated value, or null if the property is not present
     */
    public Object getValueFor(String property) {
        return properties.get(property);
    }

    /**
     * Adds a property to this object
     *
     * @param property The property's name
     * @param value    The property's associated value
     */
    public void addProperty(String property, Object value) {
        properties.put(property, value);
    }

    /**
     * Initializes this structure
     *
     * @param tabSize      Size of a tab in spaces
     * @param insertSpaces Prefer spaces over tabs
     */
    public FormattingOptions(int tabSize, boolean insertSpaces) {
        this.tabSize = tabSize;
        this.insertSpaces = insertSpaces;
        this.properties = new HashMap<>();
    }

    /**
     * Initializes this structure
     *
     * @param definition   The serialized definition
     * @param deserializer The current deserializer
     */
    public FormattingOptions(ASTNode definition, JsonDeserializer deserializer) {
        int tabSize = 0;
        boolean insertSpaces = false;
        this.properties = new HashMap<>();
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "tabSize": {
                    if (nodeValue.getSymbol().getID() == JsonLexer.ID.LITERAL_INTEGER)
                        tabSize = Integer.parseInt(nodeValue.getValue());
                    break;
                }
                case "insertSpaces": {
                    if (nodeValue.getSymbol().getID() == JsonLexer.ID.LITERAL_TRUE)
                        insertSpaces = true;
                    else if (nodeValue.getSymbol().getID() == JsonLexer.ID.LITERAL_FALSE)
                        insertSpaces = false;
                    break;
                }
                default: {
                    Object value = deserializer.deserialize(nodeValue, this);
                    properties.put(name, value);
                    break;
                }
            }
        }
        this.tabSize = tabSize;
        this.insertSpaces = insertSpaces;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"tabSize\": ");
        builder.append(Integer.toString(tabSize));
        builder.append(", \"insertSpaces\": ");
        builder.append(Boolean.toString(insertSpaces));
        for (Map.Entry<String, Object> mapping : properties.entrySet()) {
            builder.append(", ");
            builder.append("\"");
            builder.append(TextUtils.escapeStringJSON(mapping.getKey()));
            builder.append("\": ");
            Json.serialize(builder, mapping.getValue());
        }
        builder.append("}");
        return builder.toString();
    }
}
