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

/**
 * Implements a context for a single request
 *
 * @author Laurent Wouters
 */
public class JsonRpcContextSingle implements JsonRpcContext {
    /**
     * The method for the context
     */
    private final String method;

    /**
     * Initializes this context
     *
     * @param method The method for the context
     */
    public JsonRpcContextSingle(String method) {
        this.method = method;
    }

    @Override
    public boolean isBatch() {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return method == null;
    }

    @Override
    public String getMethodFor(String requestId) {
        return method;
    }
}
