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
 * The parameters of a Workspace Symbol Request
 *
 * @author Laurent Wouters
 */
public class WorkspaceSymbolParams implements Serializable {
    /**
     * A non-empty query string
     */
    protected final String query;

    /**
     * Gets the query string
     *
     * @return The query string
     */
    public String getQuery() {
        return query;
    }

    /**
     * Initializes this structure
     *
     * @param query A non-empty query string
     */
    public WorkspaceSymbolParams(String query) {
        this.query = query;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public WorkspaceSymbolParams(ASTNode definition) {
        String query = "";
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "query": {
                    query = TextUtils.unescape(nodeValue.getValue());
                    query = query.substring(1, query.length() - 1);
                    break;
                }
            }
        }
        this.query = query;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"query\": \"" +
                TextUtils.escapeStringJSON(query) +
                "\"}";
    }
}
