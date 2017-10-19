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

package fr.cenotelie.commons.utils.json;

import fr.cenotelie.commons.utils.Serializable;
import fr.cenotelie.commons.utils.TextUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents an object that has been serialized but is not recognized as an object of a known type when de-serialized
 *
 * @author Laurent Wouters
 */
public class SerializedUnknown implements Serializable {
    /**
     * The properties for this object
     */
    private final Map<String, Object> properties;

    /**
     * Initializes this structure
     */
    public SerializedUnknown() {
        properties = new HashMap<>();
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

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        boolean first = true;
        for (Map.Entry<String, Object> mapping : properties.entrySet()) {
            if (!first)
                builder.append(", ");
            first = false;
            builder.append("\"");
            builder.append(TextUtils.escapeStringJSON(mapping.getKey()));
            builder.append("\": ");
            Json.serialize(builder, mapping.getValue());
        }
        builder.append("}");
        return builder.toString();
    }
}
