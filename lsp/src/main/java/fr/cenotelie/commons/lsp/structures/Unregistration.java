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
 * General parameters to unregister a capability
 *
 * @author Laurent Wouters
 */
public class Unregistration implements Serializable {
    /**
     * The id used to unregister the request or notification.
     * Usually an id provided during the register request.
     */
    private final String id;
    /**
     * The method / capability to unregister for.
     */
    private final String method;

    /**
     * Initializes this structure
     *
     * @param id     The id used to unregister the request or notification.
     * @param method The method / capability to unregister for
     */
    public Unregistration(String id, String method) {
        this.id = id;
        this.method = method;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public Unregistration(ASTNode definition) {
        String id = "";
        String method = "";
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
            }
        }
        this.id = id;
        this.method = method;
    }

    /**
     * Gets the id used to unregister the request or notification.
     *
     * @return The id used to unregister the request or notification.
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the method / capability to unregister for
     *
     * @return The method / capability to unregister for
     */
    public String getMethod() {
        return method;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"id\": \"" +
                TextUtils.escapeStringJSON(id) +
                "\", \"method\": \"" +
                TextUtils.escapeStringJSON(method) +
                "\"}";
    }
}

