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
import fr.cenotelie.commons.utils.json.JsonParser;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * Represents a parameter of a callable-signature.
 * A parameter can have a label and a doc-comment.
 *
 * @author Laurent Wouters
 */
public class ParameterInformation implements Serializable {
    /**
     * The label of this parameter.
     * Will be shown in the UI.
     */
    private final String label;
    /**
     * The human-readable doc-comment of this parameter.
     * Will be shown in the UI but can be omitted.
     */
    private final Object documentation;

    /**
     * The label of this parameter
     *
     * @return Gets he label of this parameter
     */
    public String getLabel() {
        return label;
    }

    /**
     * Gets the human-readable doc-comment of this parameter
     *
     * @return The human-readable doc-comment of this parameter
     */
    public Object getDocumentation() {
        return documentation;
    }

    /**
     * Initializes this structure
     *
     * @param label The label of this parameter
     */
    public ParameterInformation(String label) {
        this(label, (String) null);
    }

    /**
     * Initializes this structure
     *
     * @param label         The label of this parameter
     * @param documentation The human-readable doc-comment of this parameter
     */
    public ParameterInformation(String label, String documentation) {
        this.label = label;
        this.documentation = documentation;
    }

    /**
     * Initializes this structure
     *
     * @param label         The label of this parameter
     * @param documentation The human-readable doc-comment of this parameter
     */
    public ParameterInformation(String label, MarkupContent documentation) {
        this.label = label;
        this.documentation = documentation;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public ParameterInformation(ASTNode definition) {
        String label = "";
        Object documentation = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "label": {
                    label = TextUtils.unescape(nodeValue.getValue());
                    label = label.substring(1, label.length() - 1);
                    break;
                }
                case "documentation": {
                    if (nodeValue.getSymbol().getID() == JsonParser.ID.object)
                        documentation = new MarkupContent(nodeValue);
                    else {
                        String value = TextUtils.unescape(nodeValue.getValue());
                        documentation = value.substring(1, value.length() - 1);
                    }
                    break;
                }
            }
        }
        this.label = label;
        this.documentation = documentation;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"label\": \"");
        builder.append(TextUtils.escapeStringJSON(label));
        builder.append("\"");
        if (documentation != null) {
            builder.append(", \"documentation\": ");
            Json.serialize(builder, documentation);
        }
        builder.append("}");
        return builder.toString();
    }
}
