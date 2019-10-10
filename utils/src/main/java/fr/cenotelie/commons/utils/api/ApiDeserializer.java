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

package fr.cenotelie.commons.utils.api;

import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.commons.utils.json.JsonDeserializer;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * Implements a deserializer of Json-serialized API objects
 *
 * @author Laurent Wouters
 */
public class ApiDeserializer extends JsonDeserializer {
    /**
     * The associated factory
     */
    private final ApiFactory factory;

    /**
     * Initializes this deserializer
     *
     * @param factory The associated factory
     */
    public ApiDeserializer(ApiFactory factory) {
        this.factory = factory;
    }

    /**
     * Gets the associated factory
     *
     * @return The associated factory
     */
    public ApiFactory getFactory() {
        return factory;
    }

    @Override
    public Object deserializeObject(ASTNode definition, Object context) {
        // this is an object, does it have a type
        ASTNode nodeType = null;
        for (ASTNode memberNode : definition.getChildren()) {
            String memberName = TextUtils.unescape(memberNode.getChildren().get(0).getValue());
            memberName = memberName.substring(1, memberName.length() - 1);
            ASTNode memberValue = memberNode.getChildren().get(1);
            if ("type".equals(memberName)) {
                nodeType = memberValue;
            }
        }
        if (nodeType != null && factory != null) {
            // we have a type
            String type = TextUtils.unescape(nodeType.getValue());
            type = type.substring(1, type.length() - 1);
            Object result = factory.newObject(type, definition);
            if (result != null)
                return result;
        }
        return super.deserializeObject(definition, context);
    }
}
