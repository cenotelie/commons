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

import fr.cenotelie.commons.lsp.structures.Range;

/**
 * The reference to a symbol within a document
 *
 * @author Laurent Wouters
 */
public class DocumentSymbolReference {
    /**
     * The referenced symbol
     */
    private final Symbol symbol;
    /**
     * The range of the reference
     */
    private final Range range;

    /**
     * Initializes this reference
     *
     * @param symbol The referenced symbol
     * @param range  The range of the reference
     */
    public DocumentSymbolReference(Symbol symbol, Range range) {
        this.symbol = symbol;
        this.range = range;
    }

    /**
     * Gets the referenced symbol
     *
     * @return The referenced symbol
     */
    public Symbol getSymbol() {
        return symbol;
    }

    /**
     * Gets the range of the reference
     *
     * @return The range of the reference
     */
    public Range getRange() {
        return range;
    }
}
