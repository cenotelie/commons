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
 * Text documents are identified using a URI. On the protocol level, URIs are passed as strings.
 *
 * @author Laurent Wouters
 */
public class TextDocumentIdentifier implements Serializable {
    /**
     * The document's URI
     */
    protected final String uri;

    /**
     * Initializes this structure
     *
     * @param uri The document's URI
     */
    public TextDocumentIdentifier(String uri) {
        this.uri = uri;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public TextDocumentIdentifier(ASTNode definition) {
        String uri = "";
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            if ("uri".equals(name)) {
                uri = TextUtils.unescape(nodeValue.getValue());
                uri = uri.substring(1, uri.length() - 1);
            }
        }
        this.uri = uri;
    }

    /**
     * Gets the document's URI
     *
     * @return The document's URI
     */
    public String getUri() {
        return uri;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"uri\": \"" +
                TextUtils.escapeStringJSON(uri) +
                "\"}";
    }
}
