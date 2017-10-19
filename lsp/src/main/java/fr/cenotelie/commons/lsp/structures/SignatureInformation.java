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
 * Represents the signature of something callable.
 * A signature can have a label, like a function-name, a doc-comment, and a set of parameters.
 *
 * @author Laurent Wouters
 */
public class SignatureInformation implements Serializable {
    /**
     * The label of this signature.
     * Will be shown in the UI.
     */
    private final String label;
    /**
     * The human-readable doc-comment of this signature.
     * Will be shown in the UI but can be omitted.
     */
    private final String documentation;
    /**
     * The parameters of this signature
     */
    private final ParameterInformation[] parameters;

    /**
     * The label of this signature
     *
     * @return Gets he label of this signature
     */
    public String getLabel() {
        return label;
    }

    /**
     * Gets the human-readable doc-comment of this signature
     *
     * @return The human-readable doc-comment of this signature
     */
    public String getDocumentation() {
        return documentation;
    }

    /**
     * Gets the parameters of this signature
     *
     * @return The parameters of this signature
     */
    public ParameterInformation[] getParameters() {
        return parameters;
    }

    /**
     * Initializes this structure
     *
     * @param label The label of this signature
     */
    public SignatureInformation(String label) {
        this(label, null, null);
    }

    /**
     * Initializes this structure
     *
     * @param label         The label of this signature
     * @param documentation The human-readable doc-comment of this signature
     */
    public SignatureInformation(String label, String documentation) {
        this(label, documentation, null);
    }

    /**
     * Initializes this structure
     *
     * @param label         The label of this signature
     * @param documentation The human-readable doc-comment of this signature
     * @param parameters    The parameters of this signature
     */
    public SignatureInformation(String label, String documentation, ParameterInformation[] parameters) {
        this.label = label;
        this.documentation = documentation;
        this.parameters = parameters;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public SignatureInformation(ASTNode definition) {
        String label = "";
        String documentation = null;
        ParameterInformation[] parameters = null;
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
                    documentation = TextUtils.unescape(nodeValue.getValue());
                    documentation = documentation.substring(1, documentation.length() - 1);
                    break;
                }
                case "parameters": {
                    parameters = new ParameterInformation[nodeValue.getChildren().size()];
                    int index = 0;
                    for (ASTNode nodeItem : nodeValue.getChildren())
                        parameters[index++] = new ParameterInformation(nodeItem);
                    break;
                }
            }
        }
        this.label = label;
        this.documentation = documentation;
        this.parameters = parameters;
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
            builder.append(", \"documentation\": \"");
            builder.append(TextUtils.escapeStringJSON(documentation));
            builder.append("\"");
        }
        if (parameters != null) {
            builder.append(", \"parameters\": [");
            for (int i = 0; i != parameters.length; i++) {
                if (i != 0)
                    builder.append(", ");
                builder.append(parameters[i].serializedJSON());
            }
            builder.append("]");
        }
        builder.append("}");
        return builder.toString();
    }
}
