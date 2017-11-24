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

package fr.cenotelie.commons.utils.csv;

/**
 * Represents a row in a CSV document as an iterator over the value of its cells
 * This class uses a small state machine to matches the tokens against the following regular expression:
 * Row -> (Cell (Separator Cell)* )? End
 * Cell is a cell's value token in the lexer
 * Separator is a separator token in the lexer
 * End can be either the Error, EOF, or Line ending tokens
 *
 * @author Laurent Wouters
 */
class CsvRowImpl implements CsvRow {
    /**
     * The initial state
     */
    private static final int STATE_INIT = 0;
    /**
     * A token has been matched
     */
    private static final int STATE_CELL = 1;
    /**
     * A separator has been matched
     */
    private static final int STATE_SEPARATOR = 2;
    /**
     * End of line, or end of input has been reached
     */
    private static final int STATE_END = 3;

    /**
     * The CSV lexer to use
     */
    private final CsvLexer lexer;
    /**
     * The value of the next cell in this row
     */
    private String next;
    /**
     * The current state in the state machine
     */
    private int state;

    /**
     * Initializes this row
     *
     * @param lexer The CSV lexer to use
     */
    public CsvRowImpl(CsvLexer lexer) {
        this.lexer = lexer;
        this.state = 0;
        findNext();
    }

    /**
     * Executes the transitions on the initial state
     */
    public void onStateInit() {
        next = lexer.next();
        int type = lexer.getTokenType();
        if (type == CsvLexer.TOKEN_VALUE) {
            state = STATE_CELL;
        } else if (type == CsvLexer.TOKEN_SEPARATOR) {
            next = "";
            state = STATE_SEPARATOR;
        } else {
            next = null;
            state = STATE_END;
        }
    }

    /**
     * Executes the transitions on the OnCell state
     */
    private void onStateCell() {
        next = lexer.next();
        int type = lexer.getTokenType();
        if (type == CsvLexer.TOKEN_SEPARATOR) {
            onStateSeparator();
        } else {
            next = null;
            state = STATE_END;
        }
    }

    /**
     * Executes the transitions on the OnSeparator state
     */
    private void onStateSeparator() {
        next = lexer.next();
        int type = lexer.getTokenType();
        if (type == CsvLexer.TOKEN_VALUE) {
            state = STATE_CELL;
        } else if (type == CsvLexer.TOKEN_SEPARATOR) {
            next = "";
            state = STATE_SEPARATOR;
        } else {
            next = "";
            state = STATE_END;
        }
    }

    /**
     * Executes the transitions on the final state
     */
    private void onStateEnd() {
        next = null;
    }

    /**
     * Executes the state machine
     */
    private void findNext() {
        switch (state) {
            case STATE_INIT:
                onStateInit();
                break;
            case STATE_CELL:
                onStateCell();
                break;
            case STATE_SEPARATOR:
                onStateSeparator();
                break;
            default:
                onStateEnd();
                break;
        }
    }

    @Override
    public boolean hasNext() {
        return (next != null);
    }

    @Override
    public String next() {
        String value = next;
        findNext();
        return value;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
