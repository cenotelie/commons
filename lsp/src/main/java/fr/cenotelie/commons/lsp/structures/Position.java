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

package fr.cenotelie.commons.lsp.structures;

import fr.cenotelie.commons.utils.Serializable;
import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.hime.redist.ASTNode;

import java.util.Comparator;

/**
 * Position in a text document expressed as zero-based line and character offset. A position is between two characters like an 'insert' cursor in a editor.
 *
 * @author Laurent Wouters
 */
public class Position implements Serializable, Comparable<Position> {
    /**
     * The in-order comparator of positions
     */
    public static final Comparator<Position> COMPARATOR_ORDER = Position::compareTo;

    /**
     * The inverse order comparator of positions
     */
    public static final Comparator<Position> COMPARATOR_INVERSE = Comparator.reverseOrder();

    /**
     * Line position in a document (zero-based).
     */
    private final int line;
    /**
     * Character offset on a line in a document (zero-based).
     */
    private final int character;

    /**
     * Gets the line position in a document (zero-based).
     *
     * @return Line position in a document (zero-based).
     */
    public int getLine() {
        return line;
    }

    /**
     * Gets the character offset on a line in a document (zero-based).
     *
     * @return Character offset on a line in a document (zero-based).
     */
    public int getCharacter() {
        return character;
    }

    /**
     * Initializes this structure
     *
     * @param line      Line position in a document (zero-based).
     * @param character Character offset on a line in a document (zero-based).
     */
    public Position(int line, int character) {
        this.line = line;
        this.character = character;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public Position(ASTNode definition) {
        int line = 0;
        int character = 0;
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "line": {
                    line = Integer.parseInt(nodeValue.getValue());
                    break;
                }
                case "character": {
                    character = Integer.parseInt(nodeValue.getValue());
                    break;
                }
            }
        }
        this.line = line;
        this.character = character;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return "{\"line\": " +
                line +
                ",\"character\": " +
                character +
                "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;
        if (!(o instanceof Position))
            return false;
        Position other = (Position) o;
        return (this.line == other.line && this.character == other.character);
    }

    @Override
    public int compareTo(Position position) {
        int result = Integer.compare(this.line, position.line);
        if (result != 0)
            return result;
        return Integer.compare(this.character, position.character);
    }

    /**
     * Compares the this position to a range
     *
     * @param range The range to compare to
     * @return less that 0 if the position is strictly before this range, 0 if it is within this range, more that 0 if it is after this range
     */
    public int compareTo(Range range) {
        int result = compareTo(range.getStart());
        if (result < 0)
            // position is before start
            return result;
        // position is after start
        result = compareTo(range.getEnd());
        if (result > 0)
            // position is after end
            return result;
        // position is after start and before end
        return 0;
    }
}
