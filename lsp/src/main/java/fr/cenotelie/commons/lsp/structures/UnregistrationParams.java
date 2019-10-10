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
 * The parameters for the capability un registration method
 *
 * @author Laurent Wouters
 */
public class UnregistrationParams implements Serializable {
    /**
     * The un-registrations
     */
    private final Unregistration[] unregisterations;

    /**
     * Initializes this structure
     *
     * @param registrations The un-registrations
     */
    public UnregistrationParams(Unregistration[] registrations) {
        this.unregisterations = registrations;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public UnregistrationParams(ASTNode definition) {
        Unregistration[] unregisterations = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            if ("unregisterations".equals(name)) {
                unregisterations = new Unregistration[nodeValue.getChildren().size()];
                int index = 0;
                for (ASTNode registration : nodeValue.getChildren())
                    unregisterations[index++] = new Unregistration(registration);
            }
        }
        this.unregisterations = unregisterations;
    }

    /**
     * Gets the un-registrations
     *
     * @return The un-registrations
     */
    public Unregistration[] getUnregisterations() {
        return unregisterations;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"unregisterations\": [");
        if (unregisterations != null) {
            for (int i = 0; i != unregisterations.length; i++) {
                if (i != 0)
                    builder.append(", ");
                builder.append(unregisterations[i].serializedJSON());
            }
        }
        builder.append("]}");
        return builder.toString();
    }
}