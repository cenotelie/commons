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

/**
 * Describes the content type that a client supports in various result literals like `Hover`, `ParameterInfo` or `CompletionItem`.
 * Please note that `MarkupKinds` must not start with a `$`. This kinds are reserved for internal usage.
 *
 * @author Laurent Wouters
 */
public interface MarkupKind {
    /**
     * Plain text is supported as a content format
     */
    String PLAIN_TEXT = "plaintext";

    /**
     * Markdown is supported as a content format
     */
    String MARKDOWN = "markdown";
}
