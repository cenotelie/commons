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

import fr.cenotelie.commons.lsp.structures.TextEdit;

/**
 * Represents a document service that handles operations on symbols within a document
 *
 * @author Laurent Wouters
 */
public interface DocumentSymbolHandler extends DocumentService {
    /**
     * Determines whether the new name for a symbol is legal
     *
     * @param document The document from which the request originated
     * @param symbols  The symbol registry
     * @param symbol   The symbol to be renamed
     * @param newName  The new name for the symbol
     * @return Whether the new name is legal
     */
    boolean isLegalName(Document document, SymbolRegistry symbols, Symbol symbol, String newName);

    /**
     * Gets the edits necessary to rename all occurrences of a symbol in a document
     *
     * @param document The document with occurrences of the symbol to be renamed
     * @param symbol   The symbol to be renamed
     * @param newName  The new name for the symbol
     * @return The edits for the rename operation
     */
    TextEdit[] rename(Document document, Symbol symbol, String newName);
}
