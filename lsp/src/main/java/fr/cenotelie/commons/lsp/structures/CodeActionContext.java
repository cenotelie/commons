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
import fr.cenotelie.commons.utils.json.JsonParser;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * Contains additional diagnostic information about the context in which a code action is run
 *
 * @author Laurent Wouters
 */
public class CodeActionContext implements Serializable {
    /**
     * The diagnostics
     */
    private final Diagnostic[] diagnostics;

    /**
     * Gets the diagnostics
     *
     * @return The diagnostics
     */
    public Diagnostic[] getDiagnostics() {
        return diagnostics;
    }

    /**
     * Initializes this structure
     *
     * @param diagnostics The diagnostics
     */
    public CodeActionContext(Diagnostic[] diagnostics) {
        this.diagnostics = diagnostics;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public CodeActionContext(ASTNode definition) {
        Diagnostic[] diagnostics = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "diagnostics": {
                    if (nodeValue.getSymbol().getID() == JsonParser.ID.array) {
                        diagnostics = new Diagnostic[nodeValue.getChildren().size()];
                        int index = 0;
                        for (ASTNode nodeItem : nodeValue.getChildren())
                            diagnostics[index++] = new Diagnostic(nodeItem);
                    }
                    break;
                }
            }
        }
        this.diagnostics = diagnostics == null ? new Diagnostic[0] : diagnostics;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"diagnostics\": [");
        for (int i = 0; i != diagnostics.length; i++) {
            if (i != 0)
                builder.append(", ");
            builder.append(diagnostics[i].serializedJSON());
        }
        builder.append("]}");
        return builder.toString();
    }
}
