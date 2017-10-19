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

import fr.cenotelie.commons.lsp.structures.Diagnostic;
import fr.cenotelie.commons.lsp.structures.DocumentLink;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the result of a document analysis
 *
 * @author Laurent Wouters
 */
public class DocumentAnalysis {
    /**
     * The version of the document used for this analysis
     */
    protected final DocumentVersion version;
    /**
     * The symbols for the document
     */
    protected final DocumentSymbols symbols;
    /**
     * The diagnostics for the document
     */
    protected final List<Diagnostic> diagnostics;
    /**
     * The links within the document
     */
    protected final List<DocumentLink> links;
    /**
     * Whether the document analysis has been successfully completed
     */
    protected boolean isSuccessful;

    /**
     * Gets the version of the document used for this analysis
     *
     * @return The version of the document used for this analysis
     */
    public DocumentVersion getVersion() {
        return version;
    }

    /**
     * Gets the symbols for the document
     *
     * @return The symbols for the document
     */
    public DocumentSymbols getSymbols() {
        return symbols;
    }

    /**
     * The diagnostics for the document
     *
     * @return The diagnostics for the document
     */
    public List<Diagnostic> getDiagnostics() {
        return diagnostics;
    }

    /**
     * Gets the links within the document
     *
     * @return The links within the document
     */
    public List<DocumentLink> getLinks() {
        return links;
    }

    /**
     * Gets whether the document analysis has been successfully completed
     *
     * @return Whether the document analysis has been successfully completed
     */
    public boolean isSuccessful() {
        return isSuccessful;
    }

    /**
     * Sets whether the document analysis has been successfully completed
     *
     * @param isSuccessful Whether the document analysis has been successfully completed
     */
    public void setIsSuccessful(boolean isSuccessful) {
        this.isSuccessful = isSuccessful;
    }

    /**
     * Initializes this analysis
     *
     * @param version The version of the document used for this analysis
     */
    public DocumentAnalysis(DocumentVersion version) {
        this.version = version;
        this.symbols = new DocumentSymbols();
        this.diagnostics = new ArrayList<>();
        this.links = new ArrayList<>();
        this.isSuccessful = false;
    }
}
