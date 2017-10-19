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

package fr.cenotelie.commons.lsp.engine;

import fr.cenotelie.commons.lsp.structures.FormattingOptions;
import fr.cenotelie.commons.lsp.structures.Position;
import fr.cenotelie.commons.lsp.structures.Range;
import fr.cenotelie.commons.lsp.structures.TextEdit;

/**
 * A service for formatting documents or parts of documents
 *
 * @author Laurent Wouters
 */
public interface DocumentFormatter extends DocumentService {
    /**
     * Formats a whole document
     *
     * @param options  The formatting options
     * @param document The document to format
     * @return The edits representing the formatting result
     */
    TextEdit[] format(FormattingOptions options, Document document);

    /**
     * Formats a range in document
     *
     * @param options  The formatting options
     * @param document The document to format
     * @param range    The range to format
     * @return The edits representing the formatting result
     */
    TextEdit[] format(FormattingOptions options, Document document, Range range);

    /**
     * Formats a part of the document when a character has been typed
     *
     * @param options   The formatting options
     * @param document  The document to format
     * @param position  The position at which this request was sent
     * @param character The character that triggered the request
     * @return The edits representing the formatting result
     */
    TextEdit[] format(FormattingOptions options, Document document, Position position, String character);
}
