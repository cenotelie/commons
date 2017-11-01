package fr.cenotelie.commons.utils.csv;

/**
 * Represents a CSV document as an iterator over its rows
 *
 * @author Laurent Wouters
 */
class CSVDocumentImpl implements CSVDocument {
    /**
     * The CSV lexer to use
     */
    private final CSVLexer lexer;

    /**
     * Initializes this document
     *
     * @param lexer The CSV lexer to use
     */
    public CSVDocumentImpl(CSVLexer lexer) {
        this.lexer = lexer;
    }

    @Override
    public boolean hasNext() {
        return (lexer.getTokenType() != CSVLexer.TOKEN_EOF);
    }

    @Override
    public CSVRow next() {
        return new CSVRowImpl(lexer);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
