/*******************************************************************************
 * Copyright (c) 2016 Association Cénotélie (cenotelie.fr)
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

import java.io.Reader;

/**
 * Represents a parser of CSV document
 * This parser implements the following grammar:
 * Document -&gt; Row (LineEnding Row)* EOF
 * Row -&gt; ( Cell (Separator Cell)* )?
 * <p>
 * An empty document is matched as having a single row with no cell.
 * A line ending just before the EOF also represents an empty row.
 *
 * @author Laurent Wouters
 */
public class CSV {
    /**
     * Parses a CSV document
     *
     * @param input          The input to parse
     * @param valueSeparator The character that separates values in rows
     * @param textMarker     The character that marks the beginning and end of raw text
     */
    public static CSVDocument parse(Reader input, char valueSeparator, char textMarker) {
        return parse(input, valueSeparator, textMarker, false);
    }

    /**
     * Parses a CSV document
     *
     * @param input                   The input to parse
     * @param valueSeparator          The character that separates values in rows
     * @param textMarker              The character that marks the beginning and end of raw text
     * @param keepBeginningWhiteSpace Whether the beginning string whitespaces must be kept or removed
     */
    public static CSVDocument parse(final Reader input, final char valueSeparator, final char textMarker, final boolean keepBeginningWhiteSpace) {
        CSVLexer lexer = new CSVLexer(input, valueSeparator, textMarker, keepBeginningWhiteSpace);
        return new CSVDocumentImpl(lexer);
    }
}
