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
 * General parameters to register for a capability
 *
 * @author Laurent Wouters
 */
public class Registration implements Serializable {
    /**
     * The id used to register the request.
     * The id can be used to deregister the request again.
     */
    private final String id;
    /**
     * The method / capability to register for.
     */
    private final String method;
    /**
     * Options necessary for the registration
     */
    private final Object registerOptions;

    /**
     * Initializes this structure
     *
     * @param id              The id used to register the request
     * @param method          The method / capability to register for
     * @param registerOptions Options necessary for the registration
     */
    public Registration(String id, String method, Object registerOptions) {
        this.id = id;
        this.method = method;
        this.registerOptions = registerOptions;
    }

    /**
     * Initializes this structure
     *
     * @param definition   The serialized definition
     * @param deserializer The deserializer to use
     */
    public Registration(ASTNode definition, JsonDeserializer deserializer) {
        String id = "";
        String method = "";
        Object registerOptions = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "id": {
                    id = TextUtils.unescape(nodeValue.getValue());
                    id = id.substring(1, id.length() - 1);
                    break;
                }
                case "method": {
                    method = TextUtils.unescape(nodeValue.getValue());
                    method = method.substring(1, method.length() - 1);
                    break;
                }
                case "registerOptions": {
                    registerOptions = deserializer.deserialize(nodeValue, this);
                    break;
                }
            }
        }
        this.id = id;
        this.method = method;
        this.registerOptions = registerOptions;
    }

    /**
     * Gets the id used to register the request
     *
     * @return The id used to register the request
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the method / capability to register for
     *
     * @return The method / capability to register for
     */
    public String getMethod() {
        return method;
    }

    /**
     * Gets the options necessary for the registration
     *
     * @return The options necessary for the registration
     */
    public Object getRegisterOptions() {
        return registerOptions;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"id\": \"");
        builder.append(TextUtils.escapeStringJSON(id));
        builder.append("\", \"message\": \"");
        builder.append(TextUtils.escapeStringJSON(method));
        builder.append("\"");
        if (registerOptions != null) {
            builder.append(", \"registerOptions\": ");
            Json.serialize(builder, registerOptions);
        }
        builder.append("}");
        return builder.toString();
    }
}
