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

import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * Markdown implementation of a MarkedString
 *
 * @author Laurent Wouters
 */
public class MarkedStringMarkdown implements MarkedString {
    /**
     * The content Markdown string
     */
    private final String content;

    /**
     * Initializes this structure
     *
     * @param content The content Markdown string
     */
    public MarkedStringMarkdown(String content) {
        this.content = content;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public MarkedStringMarkdown(ASTNode definition) {
        String value = TextUtils.unescape(definition.getValue());
        this.content = value.substring(1, value.length() - 1);
    }

    @Override
    public boolean isMarkdown() {
        return true;
    }

    @Override
    public String serializedString() {
        return content;
    }

    @Override
    public String serializedJSON() {
        return "\"" + TextUtils.escapeStringJSON(content) + "\"";
    }
}
