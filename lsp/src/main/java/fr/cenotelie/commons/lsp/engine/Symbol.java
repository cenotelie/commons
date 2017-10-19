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
import fr.cenotelie.commons.lsp.structures.MarkedString;
import fr.cenotelie.commons.lsp.structures.Position;
import fr.cenotelie.commons.lsp.structures.Range;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the data of a symbol
 *
 * @author Laurent Wouters
 */
public class Symbol {
    /**
     * The unique identifier of this symbol
     */
    private final String identifier;
    /**
     * The name of this symbol
     */
    private String name;
    /**
     * The kind of this symbol
     */
    private int kind;
    /**
     * The documentation for this symbol
     */
    private MarkedString documentation;
    /**
     * The various definitions of this symbol by the URI of the defining files
     */
    private final Map<String, Collection<Range>> definitions;
    /**
     * The various references to this symbol by the URI of the referencing files
     */
    private final Map<String, Collection<Range>> references;
    /**
     * The parent symbol, if any
     */
    private Symbol parent;

    /**
     * Initializes this symbol
     *
     * @param identifier The unique identifier of this symbol
     */
    public Symbol(String identifier) {
        this.identifier = identifier;
        this.name = identifier;
        this.kind = 0;
        this.documentation = null;
        this.definitions = new HashMap<>();
        this.references = new HashMap<>();
        this.parent = null;
    }

    /**
     * Gets whether this symbol still exists
     *
     * @return Whether this symbol still exists
     */
    public boolean exists() {
        return (!definitions.isEmpty() || !references.isEmpty());
    }

    /**
     * Gets the unique identifier of this symbol
     *
     * @return The unique identifier
     */
    public String getIdentifier() {
        return identifier;
    }

    /**
     * Gets the name of this symbol
     *
     * @return The name of this symbol
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the kind of this symbol
     *
     * @return The kind of this symbol
     */
    public int getKind() {
        return kind;
    }

    /**
     * Gets the documentation for this symbol
     *
     * @return The documentation for this symbol
     */
    public MarkedString getDocumentation() {
        return documentation;
    }

    /**
     * Gets the URI of the documents that defines this symbol
     *
     * @return The URI of the defining documents
     */
    public Collection<String> getDefiningDocuments() {
        return definitions.keySet();
    }

    /**
     * Gets the location of the definitions of this symbol within a document
     *
     * @param uri The URI of the document
     * @return The location of the definitions of this symbol within the document
     */
    public Collection<Range> getDefinitionsIn(String uri) {
        return definitions.get(uri);
    }

    /**
     * Gets all the definitions of this symbol
     *
     * @return The definitions
     */
    public Collection<Location> getDefinitions() {
        Collection<Location> result = new ArrayList<>();
        for (Map.Entry<String, Collection<Range>> entry : definitions.entrySet()) {
            for (Range range : entry.getValue()) {
                result.add(new Location(entry.getKey(), range));
            }
        }
        return result;
    }

    /**
     * Gets the URI of the documents that reference this symbol
     *
     * @return The URI of the referencing documents
     */
    public Collection<String> getReferencingDocuments() {
        return references.keySet();
    }

    /**
     * Gets the location of the references to this symbol within a document
     *
     * @param uri The URI of the document
     * @return The location of the references to this symbol within the document
     */
    public Collection<Range> getReferencesIn(String uri) {
        return references.get(uri);
    }

    /**
     * Gets all the definitions of this symbol
     *
     * @return The definitions
     */
    public Collection<Location> getReferences() {
        Collection<Location> result = new ArrayList<>();
        for (Map.Entry<String, Collection<Range>> entry : references.entrySet()) {
            for (Range range : entry.getValue()) {
                result.add(new Location(entry.getKey(), range));
            }
        }
        return result;
    }

    /**
     * Gets the parent symbol
     *
     * @return The parent symbol
     */
    public Symbol getParent() {
        return parent;
    }

    /**
     * Sets th user name of this symbol
     *
     * @param name the user name of this symbol
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Sets the kind of this symbol
     *
     * @param kind The kind of this symbol
     */
    public void setKind(int kind) {
        this.kind = kind;
    }

    /**
     * Sets the documentation for this symbol
     *
     * @param documentation The documentation for this symbol
     */
    public void setDocumentation(MarkedString documentation) {
        this.documentation = documentation;
    }

    /**
     * Sets the parent symbol
     *
     * @param parent The parent symbol
     */
    public void setParent(Symbol parent) {
        this.parent = parent;
    }

    /**
     * Adds a definition of this symbol
     *
     * @param uri      The defining document
     * @param location The location in the document
     */
    public void addDefinition(String uri, Range location) {
        Collection<Range> locations = definitions.get(uri);
        if (locations == null) {
            locations = new ArrayList<>();
            definitions.put(uri, locations);
        }
        locations.add(location);
    }

    /**
     * Adds a reference to this symbol
     *
     * @param uri      The referencing document
     * @param location The location in the document
     */
    public void addReference(String uri, Range location) {
        Collection<Range> locations = references.get(uri);
        if (locations == null) {
            locations = new ArrayList<>();
            references.put(uri, locations);
        }
        locations.add(location);
    }

    /**
     * Gets the range of the symbol's definition or reference at the specified position
     *
     * @param uri      The document
     * @param position The position in the document
     * @return The range of the symbol
     */
    public Range getRangeAt(String uri, Position position) {
        Collection<Range> locations = definitions.get(uri);
        if (locations != null) {
            for (Range range : locations) {
                if (position.compareTo(range) == 0)
                    return range;
            }
        }
        locations = references.get(uri);
        if (locations != null) {
            for (Range range : locations) {
                if (position.compareTo(range) == 0)
                    return range;
            }
        }
        return null;
    }

    /**
     * When the specified file has been removed
     *
     * @param uri The uri of the removed file
     * @return Whether the symbol still exists
     */
    public boolean onFileRemoved(String uri) {
        definitions.remove(uri);
        references.remove(uri);
        return exists();
    }
}
