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

import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.commons.utils.json.Json;

/**
 * Implements a Json-Rpc response with a result
 *
 * @author Laurent Wouters
 */
public class JsonRpcResponseResult<T> implements JsonRpcResponse {
    /**
     * The identifier for the corresponding request
     */
    private final String identifier;
    /**
     * The result data
     */
    private final T result;

    /**
     * Initializes this response
     *
     * @param identifier The identifier for the corresponding request
     * @param result     The result data
     */
    public JsonRpcResponseResult(String identifier, T result) {
        if (identifier == null)
            throw new IllegalArgumentException("identifier must not be null");
        this.identifier = identifier;
        this.result = result;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public boolean isError() {
        return false;
    }

    /**
     * Gets the result data
     *
     * @return The result data
     */
    public T getResult() {
        return result;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"jsonrpc\": \"2.0\", \"id\": \"");
        builder.append(TextUtils.escapeStringJSON(identifier));
        builder.append("\", \"result\": ");
        Json.serialize(builder, result);
        builder.append("}");
        return builder.toString();
    }
}
