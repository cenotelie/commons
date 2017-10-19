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

/**
 * Represents an entity that can analyze the content of a file
 *
 * @author Laurent Wouters
 */
public interface DocumentAnalyzer extends DocumentService {
    /**
     * The error code for a complete failure of the parser to produce output
     */
    String CODE_PARSER_FAILURE = "xowl-0";
    /**
     * The error code for parsing errors
     */
    String CODE_PARSING_ERROR = "xowl-1";

    /**
     * Analyzes this document
     *
     * @param factory  The factory for symbols
     * @param document The document to analyze
     * @return The analysis
     */
    DocumentAnalysis analyze(SymbolFactory factory, Document document);
}
