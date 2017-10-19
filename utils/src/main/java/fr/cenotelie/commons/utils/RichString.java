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

package fr.cenotelie.commons.utils;

import fr.cenotelie.commons.utils.json.Json;

/**
 * Represents a rich string for a message with possible links to objects
 * This class is expected to be used to represented rich messages in a user interface.
 *
 * @author Laurent Wouters
 */
public class RichString implements Serializable {
    /**
     * The style for a font
     */
    public enum FontStyle {
        normal, bold, italic
    }

    /**
     * The size for a font
     */
    public enum FontSize {
        smallest,
        smaller,
        small,
        normal,
        big,
        bigger,
        biggest
    }

    /**
     * Implements a span in a rich string that has a visual style
     */
    public static class StyledSpan implements Serializable {
        /**
         * The value for this span
         */
        private final Object value;
        /**
         * The font style
         */
        private final FontStyle fontStyle;
        /**
         * The font size
         */
        private final FontSize fontSize;
        /**
         * The color code
         */
        private final int color;

        /**
         * Initializes this span
         *
         * @param value     The value for this span
         * @param fontStyle The font style
         * @param fontSize  The font size
         * @param color     The color code
         */
        public StyledSpan(Object value, FontStyle fontStyle, FontSize fontSize, int color) {
            this.value = value;
            this.fontStyle = fontStyle;
            this.fontSize = fontSize;
            this.color = color;
        }

        @Override
        public String serializedString() {
            if (value instanceof Serializable)
                return ((Serializable) value).serializedString();
            return value.toString();
        }

        @Override
        public String serializedJSON() {
            return "{\"type\": \"" +
                    TextUtils.escapeStringJSON(StyledSpan.class.getCanonicalName()) +
                    "\", \"value\": " +
                    Json.serialize(value) +
                    ", \"fontStyle\": \"" +
                    TextUtils.escapeStringJSON(fontStyle.toString()) +
                    "\", \"fontSize\": \"" +
                    TextUtils.escapeStringJSON(fontSize.toString()) +
                    "\", \"color\": \"" +
                    Integer.toString(color) +
                    "\"}";
        }
    }

    /**
     * The parts of this string
     */
    private final Object[] parts;

    /**
     * Initializes this string
     *
     * @param parts The parts of this string
     */
    public RichString(Object... parts) {
        this.parts = parts;
    }

    @Override
    public String serializedString() {
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i != parts.length; i++) {
            if (parts[i] instanceof Serializable)
                buffer.append(((Serializable) parts[i]).serializedString());
            else
                buffer.append(parts[i].toString());
        }
        return buffer.toString();
    }

    @Override
    public String serializedJSON() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("{\"type\": \"");
        buffer.append(TextUtils.escapeStringJSON(RichString.class.getCanonicalName()));
        buffer.append("\", \"parts\": [");
        for (int i = 0; i != parts.length; i++) {
            if (i != 0)
                buffer.append(", ");
            Json.serialize(buffer, parts[i]);
        }
        buffer.append("]}");
        return buffer.toString();
    }

    @Override
    public String toString() {
        return serializedString();
    }
}
