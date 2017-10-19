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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Implements a context for a batch of requests
 *
 * @author Laurent Wouters
 */
public class JsonRpcContextBatch implements JsonRpcContext {
    /**
     * The methods for the requests
     */
    private final Map<String, String> methods;

    /**
     * Initializes this context
     */
    public JsonRpcContextBatch() {
        this.methods = new HashMap<>();
    }

    /**
     * Initializes this context
     *
     * @param requests The requests
     */
    public JsonRpcContextBatch(Collection<JsonRpcRequest> requests) {
        this.methods = new HashMap<>();
        for (JsonRpcRequest request : requests)
            if (!request.isNotification())
                methods.put(request.getIdentifier(), request.getMethod());
    }

    /**
     * Adds a method to this context
     *
     * @param requestId The identifier of a request
     * @param method    The corresponding method
     */
    public void addMethod(String requestId, String method) {
        this.methods.put(requestId, method);
    }

    @Override

    public boolean isBatch() {
        return true;
    }

    @Override
    public boolean isEmpty() {
        return methods.isEmpty();
    }

    @Override
    public String getMethodFor(String requestId) {
        return methods.get(requestId);
    }
}
