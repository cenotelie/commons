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
 * A `MarkupContent` literal represents a string value which content is interpreted base on its kind flag.
 * Currently the protocol supports `plaintext` and `markdown` as markup kinds.
 * <p>
 * If the kind is `markdown` then the value can contain fenced code blocks like in GitHub issues.
 * See https://help.github.com/articles/creating-and-highlighting-code-blocks/#syntax-highlighting
 * <p>
 * Here is an example how such a string can be constructed using JavaScript / TypeScript:
 * ```ts
 * let markdown: MarkdownContent = {
 * kind: MarkupKind.Markdown,
 * value: [
 * '# Header',
 * 'Some text',
 * '```typescript',
 * 'someCode();',
 * '```'
 * ].join('\n')
 * };
 * ```
 * <p>
 * *Please Note* that clients might sanitize the return markdown.
 * A client could decide to remove HTML from the markdown to avoid script execution.
 */
public class MarkupContent implements Serializable {
    /**
     * The type of the Markup
     */
    private final String kind;
    /**
     * The content itself
     */
    private final String content;

    /**
     * Gets the type of the Markup
     *
     * @return The type of the Markup
     */
    public String getKind() {
        return kind;
    }

    /**
     * Gets the content itself
     *
     * @return The content itself
     */
    public String getContent() {
        return content;
    }

    /**
     * Initializes this structure
     *
     * @param kind    The type of the Markup
     * @param content The content itself
     */
    public MarkupContent(String kind, String content) {
        this.kind = kind;
        this.content = content;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public MarkupContent(ASTNode definition) {
        String kind = MarkupKind.PLAIN_TEXT;
        String content = "";
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "kind": {
                    kind = TextUtils.unescape(nodeValue.getValue());
                    kind = kind.substring(1, kind.length() - 1);
                    break;
                }
                case "content": {
                    content = TextUtils.unescape(nodeValue.getValue());
                    content = content.substring(1, content.length() - 1);
                    break;
                }
            }
        }
        this.kind = kind;
        this.content = content;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"kind\": \"" +
                TextUtils.escapeStringJSON(kind) +
                "\", \"content\": \"" +
                TextUtils.escapeStringJSON(content) +
                "\"}";
    }
}
