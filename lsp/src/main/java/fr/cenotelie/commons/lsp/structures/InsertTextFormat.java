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
 * Defines whether the insert text in a completion item should be interpreted as plain text or a snippet
 *
 * @author Laurent Wouters
 */
public interface InsertTextFormat {
    /**
     * The primary text to be inserted is treated as a plain string.
     */
    int PLAIN_TEXT = 1;

    /**
     * The primary text to be inserted is treated as a snippet.
     * <p>
     * A snippet can define tab stops and placeholders with `$1`, `$2`
     * and `${3:foo}`. `$0` defines the final tab stop, it defaults to
     * the end of the snippet. Placeholders with equal identifiers are linked,
     * that is typing in one will update others too.
     * </p>
     *
     * @see "https://github.com/Microsoft/vscode/blob/master/src/vs/editor/contrib/snippet/common/snippet.md"
     */
    int SNIPPET = 2;
}
