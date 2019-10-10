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
 * The code block implementation of a marked string.
 * The language identifier is semantically equal to the optional language identifier in fenced code blocks in GitHub issues.
 * See https://help.github.com/articles/creating-and-highlighting-code-blocks/#syntax-highlighting
 *
 * @author Laurent Wouters
 */
public class MarkedStringCodeBlock implements MarkedString {
    /**
     * The language identifier
     */
    private final String language;
    /**
     * The value of the code block
     */
    private final String value;

    /**
     * Initializes this structure
     *
     * @param language The language identifier
     * @param value    The value of the code block
     */
    public MarkedStringCodeBlock(String language, String value) {
        this.language = language;
        this.value = value;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public MarkedStringCodeBlock(ASTNode definition) {
        String language = "";
        String value = "";
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "language": {
                    language = TextUtils.unescape(nodeValue.getValue());
                    language = language.substring(1, language.length() - 1);
                    break;
                }
                case "value": {
                    value = TextUtils.unescape(nodeValue.getValue());
                    value = value.substring(1, value.length() - 1);
                    break;
                }
            }
        }
        this.language = language;
        this.value = value;
    }

    /**
     * Gets the language identifier
     *
     * @return The language identifier
     */
    public String getLanguage() {
        return language;
    }

    /**
     * Gets the value of the code block
     *
     * @return The value of the code block
     */
    public String getValue() {
        return value;
    }

    @Override
    public boolean isMarkdown() {
        return false;
    }

    @Override
    public String serializedString() {
        return value;
    }

    @Override
    public String serializedJSON() {
        return "{\"language\": \"" +
                TextUtils.escapeStringJSON(language) +
                "\", \"value\": \"" +
                TextUtils.escapeStringJSON(value) +
                "\"}";
    }
}
