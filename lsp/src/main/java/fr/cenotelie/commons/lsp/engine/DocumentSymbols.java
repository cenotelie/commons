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

import java.util.ArrayList;
import java.util.Collection;

/**
 * Repository of the symbols found in a document
 *
 * @author Laurent Wouters
 */
public class DocumentSymbols {
    /**
     * The references to symbols within this document
     */
    private final Collection<DocumentSymbolReference> references;
    /**
     * The definitions of symbols within this document
     */
    private final Collection<DocumentSymbolReference> definitions;

    /**
     * Initializes this structure
     */
    public DocumentSymbols() {
        this.references = new ArrayList<>();
        this.definitions = new ArrayList<>();
    }

    /**
     * Gets the references to symbols within this document
     * The references are assumed to be in order
     *
     * @return The references to symbols within this document
     */
    public Collection<DocumentSymbolReference> getReferences() {
        return references;
    }

    /**
     * Gets the definitions of symbols within this document
     * The definitions are assumed to be in order
     *
     * @return The definitions of symbols within this document
     */
    public Collection<DocumentSymbolReference> getDefinitions() {
        return definitions;
    }

    /**
     * Adds a reference to a symbol
     *
     * @param reference The reference to the symbol
     */
    public void addReference(DocumentSymbolReference reference) {
        this.references.add(reference);
    }

    /**
     * Adds the definition of a symbol
     *
     * @param reference The reference to the definition
     */
    public void addDefinition(DocumentSymbolReference reference) {
        this.definitions.add(reference);
    }
}
