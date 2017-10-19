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
import fr.cenotelie.commons.lsp.structures.DiagnosticSeverity;
import fr.cenotelie.commons.lsp.structures.Position;
import fr.cenotelie.commons.lsp.structures.Range;
import fr.cenotelie.commons.utils.Identifiable;
import fr.cenotelie.hime.redist.*;

import java.io.Reader;
import java.util.Objects;

/**
 * Basic implementation of an analyzer that use a Hime parser
 *
 * @author Laurent Wouters
 */
public abstract class DocumentAnalyzerHime implements Identifiable, DocumentAnalyzer {
    /**
     * The unique identifier for this analyzer
     */
    protected final String identifier;
    /**
     * The human readable name for the analyzer
     */
    protected final String name;
    /**
     * The language to match for the analyzer
     */
    protected final String language;

    /**
     * Initializes this analyzer
     *
     * @param identifier The unique identifier for this analyzer
     * @param name       The human readable name for the analyzer
     * @param language   The language to match for the analyzer
     */
    public DocumentAnalyzerHime(String identifier, String name, String language) {
        this.identifier = identifier;
        this.name = name;
        this.language = language;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getPriorityFor(Document document) {
        if (Objects.equals(document.getLanguageId(), language))
            return PRIORITY_HIGH;
        return PRIORITY_NONE;
    }

    @Override
    public DocumentAnalysis analyze(SymbolFactory factory, Document document) {
        DocumentAnalysisHime analysis = newAnalysis(document);
        ParseResult result = parse(document.getCurrentVersion().getContent().getReader());
        if (result == null) {
            analysis.getDiagnostics().add(new Diagnostic(
                    new Range(new Position(0, 0), new Position(0, 0)),
                    DiagnosticSeverity.ERROR,
                    CODE_PARSER_FAILURE,
                    name,
                    "The analysis failed"
            ));
            analysis.setIsSuccessful(false);
            return analysis;
        }

        for (ParseError error : result.getErrors()) {
            analysis.getDiagnostics().add(new Diagnostic(
                    new Range(
                            new Position(error.getPosition().getLine() - 1, error.getPosition().getColumn() - 1),
                            new Position(error.getPosition().getLine() - 1, error.getPosition().getColumn() - 1 + error.getLength() + 1)
                    ),
                    DiagnosticSeverity.ERROR,
                    CODE_PARSING_ERROR,
                    name,
                    error.getMessage()
            ));
        }
        analysis.setParseResult(result);

        if (result.getRoot() != null) {
            doAnalyze(document.getUri(), result.getRoot(), result.getInput(), factory, analysis);
        }
        return analysis;
    }

    /**
     * Creates a new document analysis
     *
     * @param document The document to analyze
     * @return The new document analysis
     */
    protected DocumentAnalysisHime newAnalysis(Document document) {
        return new DocumentAnalysisHime(document.getCurrentVersion());
    }

    /**
     * Parses the content of the specified reader
     *
     * @param reader The reader to use as input
     * @return The parse result
     */
    protected abstract ParseResult parse(Reader reader);

    /**
     * Performs the document analysis
     *
     * @param resourceUri The URI of the resource
     * @param root        The AST root for the document
     * @param input       The text input that was parsed
     * @param factory     The factory for symbols
     * @param analysis    The current analysis to fill
     */
    protected abstract void doAnalyze(
            String resourceUri,
            ASTNode root,
            Text input,
            SymbolFactory factory,
            DocumentAnalysis analysis);

    /**
     * Gets the range for the specified node
     *
     * @param text The input text
     * @param node The AST node
     * @return The range
     */
    protected Range getRangeFor(Text text, ASTNode node) {
        TextSpan span = node.getSpan();
        if (span == null)
            return null;
        int indexStart = span.getIndex();
        int indexEnd = indexStart + span.getLength();
        TextPosition positionStart = text.getPositionAt(indexStart);
        TextPosition positionEnd = text.getPositionAt(indexEnd);
        return new Range(
                new Position(positionStart.getLine() - 1, positionStart.getColumn() - 1),
                new Position(positionEnd.getLine() - 1, positionEnd.getColumn() - 1)
        );
    }
}
