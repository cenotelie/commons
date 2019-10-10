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

package fr.cenotelie.commons.jsonrpc;

import fr.cenotelie.commons.utils.Serializable;
import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.commons.utils.json.Json;

/**
 * Implementation of a Json-Rpc request
 *
 * @author Laurent Wouters
 */
public class JsonRpcRequest implements Serializable {
    /**
     * The identifier of this request
     */
    private final String identifier;
    /**
     * The requested method
     */
    private final String method;
    /**
     * The parameters for this request
     */
    private final Object params;

    /**
     * Initializes this request
     *
     * @param identifier The identifier of this request
     * @param method     The requested method
     * @param params     The parameters for this request
     */
    public JsonRpcRequest(String identifier, String method, Object params) {
        this.identifier = identifier;
        this.method = method;
        this.params = params;
    }

    /**
     * Gets the identifier of this request
     *
     * @return The identifier of this request
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * Gets the requested method
     *
     * @return The requested method
     */
    public String getMethod() {
        return method;
    }

    /**
     * Gets whether this request is a protocol extension request
     *
     * @return Whether this request is a protocol extension request
     */
    public boolean isProtocolExtension() {
        return method != null && (method.startsWith("rpc.") || method.startsWith("rpc\u002E"));
    }

    /**
     * Gets whether this request is a notification
     *
     * @return whether this request is a notification
     */
    public boolean isNotification() {
        return identifier == null;
    }

    /**
     * Gets the parameters for this request
     *
     * @return The parameters for this request
     */
    public Object getParams() {
        return params;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"jsonrpc\": \"2.0\"");
        if (identifier != null) {
            builder.append(", \"id\": \"");
            builder.append(TextUtils.escapeStringJSON(identifier));
            builder.append("\"");
        }
        builder.append(", \"method\": \"");
        builder.append(TextUtils.escapeStringJSON(method));
        builder.append("\"");
        if (params != null) {
            builder.append(", \"params\": ");
            Json.serialize(builder, params);
        }
        builder.append("}");
        return builder.toString();
    }
}
