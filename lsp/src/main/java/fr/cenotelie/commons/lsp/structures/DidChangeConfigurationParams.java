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
 * Parameters for the notification of a configuration change on the client
 *
 * @author Laurent Wouters
 */
public class DidChangeConfigurationParams implements Serializable {
    /**
     * The actual changed settings
     */
    private final Object settings;

    /**
     * Gets the actual changed settings
     *
     * @return The actual changed settings
     */
    public Object getSettings() {
        return settings;
    }

    /**
     * Initializes this structure
     *
     * @param settings The actual changed settings
     */
    public DidChangeConfigurationParams(Object settings) {
        this.settings = settings;
    }

    /**
     * Initializes this structure
     *
     * @param definition   The serialized definition
     * @param deserializer The deserializer to use
     */
    public DidChangeConfigurationParams(ASTNode definition, JsonDeserializer deserializer) {
        Object settings = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "settings": {
                    settings = deserializer.deserialize(nodeValue, this);
                    break;
                }
            }
        }
        this.settings = settings;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"settings\": " +
                Json.serialize(settings) +
                "}";
    }
}
