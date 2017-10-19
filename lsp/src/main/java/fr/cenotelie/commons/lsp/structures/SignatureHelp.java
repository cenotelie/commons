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
import fr.cenotelie.commons.utils.json.JsonLexer;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * Signature help represents the signature of something callable.
 * There can be multiple signature but only one active and only one active parameter.
 *
 * @author Laurent Wouters
 */
public class SignatureHelp implements Serializable {
    /**
     * One or more signatures
     */
    private final SignatureInformation[] signatures;
    /**
     * The active signature.
     * If omitted or the value lies outside the ange of `signatures` the value defaults to zero or is ignored if `signatures.length === 0`.
     * Whenever possible implementors should make an active decision about the active signature and shouldn't rely on a default value.
     */
    private final int activeSignature;
    /**
     * The active parameter of the active signature.
     * If omitted or the value lies outside the range of `signatures[activeSignature].parameters` defaults to 0 if the active signature has parameters.
     * If the active signature has no parameters it is ignored.
     */
    private final int activeParameter;

    /**
     * Initializes this structure
     *
     * @param signatures One or more signatures
     */
    public SignatureHelp(SignatureInformation[] signatures) {
        this(signatures, 0, 0);
    }

    /**
     * Initializes this structure
     *
     * @param signatures      One or more signatures
     * @param activeSignature The active signature
     */
    public SignatureHelp(SignatureInformation[] signatures, int activeSignature) {
        this(signatures, activeSignature, 0);
    }

    /**
     * Initializes this structure
     *
     * @param signatures      One or more signatures
     * @param activeSignature The active signature
     * @param activeParameter The active parameter of the active signature
     */
    public SignatureHelp(SignatureInformation[] signatures, int activeSignature, int activeParameter) {
        this.signatures = signatures;
        this.activeSignature = activeSignature;
        this.activeParameter = activeParameter;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public SignatureHelp(ASTNode definition) {
        SignatureInformation[] signatures = null;
        int activeSignature = 0;
        int activeParameter = 0;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "signatures": {
                    signatures = new SignatureInformation[nodeValue.getChildren().size()];
                    int index = 0;
                    for (ASTNode nodeItem : nodeValue.getChildren())
                        signatures[index++] = new SignatureInformation(nodeItem);
                    break;
                }
                case "activeSignature": {
                    if (nodeValue.getSymbol().getID() == JsonLexer.ID.LITERAL_INTEGER)
                        activeSignature = Integer.parseInt(nodeValue.getValue());
                    break;
                }
                case "activeParameter": {
                    if (nodeValue.getSymbol().getID() == JsonLexer.ID.LITERAL_INTEGER)
                        activeParameter = Integer.parseInt(nodeValue.getValue());
                    break;
                }
            }
        }
        this.signatures = signatures != null ? signatures : new SignatureInformation[0];
        this.activeSignature = activeSignature;
        this.activeParameter = activeParameter;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"signatures\": [");
        for (int i = 0; i != signatures.length; i++) {
            if (i != 0)
                builder.append(", ");
            builder.append(signatures[i].serializedJSON());
        }
        builder.append("], \"activeSignature\": ");
        builder.append(Integer.toString(activeSignature));
        builder.append(", \"activeParameter\": ");
        builder.append(Integer.toString(activeParameter));
        builder.append("}");
        return builder.toString();
    }
}
