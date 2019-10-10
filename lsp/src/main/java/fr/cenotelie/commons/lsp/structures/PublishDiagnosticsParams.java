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
import fr.cenotelie.hime.redist.ASTNode;

/**
 * The parameters for the textDocument/publishDiagnostics notification to the client
 *
 * @author Laurent Wouters
 */
public class PublishDiagnosticsParams implements Serializable {
    /**
     * The URI for which diagnostic information is reported
     */
    private final String uri;
    /**
     * An array of diagnostic information items
     */
    private final Diagnostic[] diagnostics;

    /**
     * Initializes this structure
     *
     * @param uri         The URI for which diagnostic information is reported
     * @param diagnostics The diagnostic information items
     */
    public PublishDiagnosticsParams(String uri, Diagnostic[] diagnostics) {
        this.uri = uri;
        this.diagnostics = diagnostics;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public PublishDiagnosticsParams(ASTNode definition) {
        String uri = "";
        Diagnostic[] diagnostics = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "uri": {
                    uri = TextUtils.unescape(nodeValue.getValue());
                    uri = uri.substring(1, uri.length() - 1);
                    break;
                }
                case "diagnostics": {
                    diagnostics = new Diagnostic[nodeValue.getChildren().size()];
                    int index = 0;
                    for (ASTNode diagnostic : nodeValue.getChildren())
                        diagnostics[index++] = new Diagnostic(diagnostic);
                    break;
                }
            }
        }
        this.uri = uri;
        this.diagnostics = diagnostics;
    }

    /**
     * Gets the URI for which diagnostic information is reported
     *
     * @return The URI for which diagnostic information is reported
     */
    public String getUri() {
        return uri;
    }

    /**
     * Gets the diagnostic information items
     *
     * @return The diagnostic information items
     */
    public Diagnostic[] getDiagnostics() {
        return diagnostics;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"uri\": \"" +
                TextUtils.escapeStringJSON(uri) +
                "\", \"diagnostics\": " +
                Json.serialize(diagnostics) +
                "}";
    }
}
