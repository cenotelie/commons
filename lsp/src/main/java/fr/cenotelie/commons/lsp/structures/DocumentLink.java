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
 * A document link is a range in a text document that links to an internal or external resource, like another text document or a web site.
 *
 * @author Laurent Wouters
 */
public class DocumentLink implements Serializable {
    /**
     * The range this link applies to
     */
    private final Range range;
    /**
     * The uri this link points to.
     * If missing a resolve request is sent later.
     */
    private final String target;

    /**
     * Gets the range this link applies to
     *
     * @return The range this link applies to
     */
    public Range getRange() {
        return range;
    }

    /**
     * Gets the uri this link points to
     *
     * @return The uri this link points to
     */
    public String getTarget() {
        return target;
    }

    /**
     * Initializes this structure
     *
     * @param range The range this link applies to
     */
    public DocumentLink(Range range) {
        this(range, null);
    }

    /**
     * Initializes this structure
     *
     * @param range  The range this link applies to
     * @param target The uri this link points to
     */
    public DocumentLink(Range range, String target) {
        this.range = range;
        this.target = target;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public DocumentLink(ASTNode definition) {
        Range range = null;
        String target = null;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "range": {
                    range = new Range(nodeValue);
                    break;
                }
                case "target": {
                    target = TextUtils.unescape(nodeValue.getValue());
                    target = target.substring(1, target.length() - 1);
                    break;
                }
            }
        }
        this.range = range != null ? range : new Range(new Position(0, 0), new Position(0, 0));
        this.target = target;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"range\": ");
        builder.append(range.serializedJSON());
        if (target != null) {
            builder.append(", \"target\": \"");
            builder.append(TextUtils.escapeStringJSON(target));
            builder.append("\"");
        }
        builder.append("}");
        return builder.toString();
    }
}
