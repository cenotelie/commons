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
import fr.cenotelie.commons.utils.json.JsonDeserializer;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * The parameters for the capability registration method
 *
 * @author Laurent Wouters
 */
public class RegistrationParams implements Serializable {
    /**
     * The registrations
     */
    private final Registration[] registrations;

    /**
     * Gets the registrations
     *
     * @return The registrations
     */
    public Registration[] getRegistrations() {
        return registrations;
    }

    /**
     * Initializes this structure
     *
     * @param registrations The registrations
     */
    public RegistrationParams(Registration[] registrations) {
        this.registrations = registrations;
    }

    /**
     * Initializes this structure
     *
     * @param definition   The serialized definition
     * @param deserializer The deserializer to use
     */
    public RegistrationParams(ASTNode definition, JsonDeserializer deserializer) {
        Registration[] registrations = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "registrations": {
                    registrations = new Registration[nodeValue.getChildren().size()];
                    int index = 0;
                    for (ASTNode registration : nodeValue.getChildren())
                        registrations[index++] = new Registration(registration, deserializer);
                }
            }
        }
        this.registrations = registrations;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"registrations\": [");
        if (registrations != null) {
            for (int i = 0; i != registrations.length; i++) {
                if (i != 0)
                    builder.append(", ");
                builder.append(registrations[i].serializedJSON());
            }
        }
        builder.append("]}");
        return builder.toString();
    }
}
