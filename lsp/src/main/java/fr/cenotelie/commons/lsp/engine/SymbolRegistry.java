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

import fr.cenotelie.commons.lsp.structures.Location;
import fr.cenotelie.commons.lsp.structures.Position;
import fr.cenotelie.commons.lsp.structures.SymbolInformation;

import java.util.*;

/**
 * The registry of symbols found in the managed files
 *
 * @author Laurent Wouters
 */
public class SymbolRegistry implements SymbolFactory {
    /**
     * The global registry of known symbols
     */
    private final Map<String, Symbol> global;
    /**
     * The symbol references for specific documents
     */
    private final Map<String, DocumentSymbols> perDocument;

    /**
     * Initializes this registry
     */
    public SymbolRegistry() {
        this.global = new HashMap<>();
        this.perDocument = new HashMap<>();
    }

    @Override
    public Symbol resolve(String identifier) {
        Symbol symbol = global.get(identifier);
        if (symbol == null) {
            symbol = new Symbol(identifier);
            global.put(identifier, symbol);
        }
        return symbol;
    }

    @Override
    public Symbol lookup(String identifier) {
        return global.get(identifier);
    }

    /**
     * When a document has been updated
     *
     * @param document The updated document
     * @param symbols  The symbols for the document
     */
    public void onDocumentChanged(Document document, DocumentSymbols symbols) {
        if (symbols == null)
            return;
        onDocumentRemoved(document);
        perDocument.put(document.getUri(), symbols);
        for (DocumentSymbolReference definition : symbols.getDefinitions()) {
            definition.getSymbol().addDefinition(document.getUri(), definition.getRange());
        }
        for (DocumentSymbolReference reference : symbols.getReferences()) {
            reference.getSymbol().addReference(document.getUri(), reference.getRange());
        }
    }

    /**
     * When a document has been removed
     *
     * @param document The removed document
     */
    public void onDocumentRemoved(Document document) {
        Collection<String> toRemove = null;
        for (Map.Entry<String, Symbol> entry : global.entrySet()) {
            if (!entry.getValue().onFileRemoved(document.getUri())) {
                if (toRemove == null)
                    toRemove = new ArrayList<>();
                toRemove.add(entry.getKey());
            }
        }
        if (toRemove != null) {
            for (String identifier : toRemove)
                global.remove(identifier);
        }
        perDocument.remove(document.getUri());
    }

    /**
     * Gets all the definitions for a document
     *
     * @param uri The document URI
     * @return The definitions
     */
    public Collection<SymbolInformation> getDefinitionsIn(String uri) {
        DocumentSymbols symbols = perDocument.get(uri);
        if (symbols == null)
            return Collections.emptyList();
        Collection<SymbolInformation> result = new ArrayList<>();
        for (DocumentSymbolReference definition : symbols.getDefinitions()) {
            result.add(new SymbolInformation(
                    definition.getSymbol().getIdentifier(),
                    definition.getSymbol().getKind(),
                    new Location(
                            uri,
                            definition.getRange()
                    ),
                    definition.getSymbol().getParent() != null ? definition.getSymbol().getParent().getIdentifier() : null
            ));
        }
        return result;
    }

    /**
     * Search for symbol information
     *
     * @param query The query string
     * @return The found symbols
     */
    public Collection<SymbolInformation> search(String query) {
        return new ArrayList<>();
    }

    /**
     * Gets the symbol referenced or defined at a location in a document
     *
     * @param uri      The document's URI
     * @param position The position within the document
     * @return The symbol, or null if it is not found
     */
    public Symbol getSymbolAt(String uri, Position position) {
        DocumentSymbols symbols = perDocument.get(uri);
        if (symbols == null)
            return null;
        for (DocumentSymbolReference reference : symbols.getReferences()) {
            int comparison = position.compareTo(reference.getRange());
            if (comparison == 0)
                return reference.getSymbol();
            if (comparison < 0)
                // the position is before the current reference, stop here
                break;
        }
        // try the definitions
        for (DocumentSymbolReference definition : symbols.getDefinitions()) {
            int comparison = position.compareTo(definition.getRange());
            if (comparison == 0)
                return definition.getSymbol();
            if (comparison < 0)
                // the position is before the current definition, stop here
                break;
        }
        // not found
        return null;
    }
}
