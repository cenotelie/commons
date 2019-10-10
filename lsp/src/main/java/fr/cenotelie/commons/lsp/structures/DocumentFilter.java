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
 * A document filter denotes a document through properties like language, schema or pattern.
 * An example is a filter that applies to TypeScript files on disk.
 * Another example is a filter the applies to JSON files with name package.json
 *
 * @author Laurent Wouters
 */
public class DocumentFilter implements Serializable {
    /**
     * A language id, like `typescript`.
     */
    private final String language;
    /**
     * A uri scheme, like `file` or `untitled`.
     */
    private final String scheme;
    /**
     * A glob pattern, like `*.{ts,js}`.
     */
    private final String pattern;

    /**
     * Initializes this structure
     *
     * @param language A language id, like `typescript`.
     * @param scheme   A uri scheme, like `file` or `untitled`.
     * @param pattern  A glob pattern, like `*.{ts,js}`.
     */
    public DocumentFilter(String language, String scheme, String pattern) {
        this.language = language;
        this.scheme = scheme;
        this.pattern = pattern;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public DocumentFilter(ASTNode definition) {
        String language = null;
        String scheme = null;
        String pattern = null;
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
                case "scheme": {
                    scheme = TextUtils.unescape(nodeValue.getValue());
                    scheme = scheme.substring(1, scheme.length() - 1);
                    break;
                }
                case "pattern": {
                    pattern = TextUtils.unescape(nodeValue.getValue());
                    pattern = pattern.substring(1, pattern.length() - 1);
                    break;
                }
            }
        }
        this.language = language;
        this.scheme = scheme;
        this.pattern = pattern;
    }

    /**
     * Gets the language id, like `typescript`.
     *
     * @return The language id, like `typescript`.
     */
    public String getLanguage() {
        return language;
    }

    /**
     * Gets the uri scheme, like `file` or `untitled`.
     *
     * @return The uri scheme, like `file` or `untitled`.
     */
    public String getScheme() {
        return scheme;
    }

    /**
     * Gets the glob pattern, like `*.{ts,js}`.
     *
     * @return The glob pattern, like `*.{ts,js}`.
     */
    public String getPattern() {
        return pattern;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        boolean first = true;
        if (language != null) {
            builder.append("\"language\": \"");
            builder.append(TextUtils.escapeStringJSON(language));
            builder.append("\"");
            first = false;
        }
        if (scheme != null) {
            if (!first)
                builder.append(", ");
            builder.append("\"scheme\": \"");
            builder.append(TextUtils.escapeStringJSON(scheme));
            builder.append("\"");
            first = false;
        }
        if (pattern != null) {
            if (!first)
                builder.append(", ");
            builder.append("\"pattern\": \"");
            builder.append(TextUtils.escapeStringJSON(pattern));
            builder.append("\"");
        }
        builder.append("}");
        return builder.toString();
    }
}
