package fr.cenotelie.commons.utils.csv;

/**
 * Represents a CSV document as an iterator over its rows
 *
 * @author Laurent Wouters
 */
class CsvDocumentImpl implements CsvDocument {
    /**
     * The CSV lexer to use
     */
    private final CsvLexer lexer;

    /**
     * Initializes this document
     *
     * @param lexer The CSV lexer to use
     */
    public CsvDocumentImpl(CsvLexer lexer) {
        this.lexer = lexer;
    }

    @Override
    public boolean hasNext() {
        return (lexer.getTokenType() != CsvLexer.TOKEN_EOF);
    }

    @Override
    public CsvRow next() {
        return new CsvRowImpl(lexer);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
