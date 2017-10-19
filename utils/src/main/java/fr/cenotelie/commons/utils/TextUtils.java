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

/**
 * Utility APIs for text manipulation
 *
 * @author Laurent Wouters
 */
public class TextUtils {
    /**
     * String containing the escaped glyphs in absolute uris
     */
    private static final String ESCAPED_GLYPHS_ABSOLUTE_URIS = "<>\"{}|^`\\";

    /**
     * Replaces special sequences in the specified input value by the corresponding value.
     * This method is general purpose in that it supports all form of escape sequences used by various syntaxes.
     * The double double-quote escape sequence ("") representing a single double-quote character (") for the CSV syntax is not supported.
     * The supported escape sequences:
     * - \ u XXXX for unicode characters in the BMP with codepoint XXXX.
     * - \ U XXXXXXXX for unicode characters outside the BMP with codepoint XXXXXXXX.
     * - \0 for the unicode character 0
     * - \a for the unicode alert character (U+0007)
     * - \t, \b, \r, \n, \f for the corresponding control characters (tab, backspace, carriage return, line feed, form feed).
     * - \C for C, where C is any character other than 0, a, t, b, r, n, f, u and U.
     *
     * @param value A string that can contain escape sequences
     * @return The equivalent string with the escape sequences replaced by their value
     */
    public static String unescape(String value) {
        char[] buffer = new char[value.length()];
        int next = 0;
        for (int i = 0; i != value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\') {
                char n = value.charAt(i + 1);
                if (n == '0') {
                    buffer[next++] = '\u0000';
                    i++;
                } else if (n == 'a') {
                    buffer[next++] = '\u0007';
                    i++;
                } else if (n == 't') {
                    buffer[next++] = '\t';
                    i++;
                } else if (n == 'b') {
                    buffer[next++] = '\b';
                    i++;
                } else if (n == 'n') {
                    buffer[next++] = '\n';
                    i++;
                } else if (n == 'r') {
                    buffer[next++] = '\r';
                    i++;
                } else if (n == 'f') {
                    buffer[next++] = '\f';
                    i++;
                } else if (n == 'u') {
                    // \ u XXXX for unicode characters in the BMP
                    // note that any unicode character is encoded in UTF-16 in at most 2 Java char
                    // therefore the length of str cannot be more that 2
                    // therefore buffer[next++] cannot overflow
                    int codepoint = Integer.parseInt(value.substring(i + 2, i + 6), 16);
                    String str = new String(new int[]{codepoint}, 0, 1);
                    for (int j = 0; j != str.length(); j++)
                        buffer[next++] = str.charAt(j);
                    i += 5;
                } else if (n == 'U') {
                    // \ U XXXXXXXX for unicode characters outside the BMP
                    // note that any unicode character is encoded in UTF-16 in at most 2 Java char
                    // therefore the length of str cannot be more that 2
                    // therefore buffer[next++] cannot overflow
                    int codepoint = Integer.parseInt(value.substring(i + 2, i + 10), 16);
                    String str = new String(new int[]{codepoint}, 0, 1);
                    for (int j = 0; j != str.length(); j++)
                        buffer[next++] = str.charAt(j);
                    i += 9;
                } else {
                    // \C for C, where C is any character other than 0, a, t, b, r, n, f, u and U
                    buffer[next++] = n;
                    i++;
                }
            } else {
                // not the start of an escape sequence, replace as is
                buffer[next++] = c;
            }
        }
        return new String(buffer, 0, next);
    }

    /**
     * Escapes special characters in the specified absolute URI according to the common W3C requirements for Turtle, N-Triples, N-quads, etc.
     * All characters are copied as-is, except for the following, which are changed for a unicode escape sequence \ u XXXX:
     * characters in range U+0000 to U+0020 and &lt;, &gt;, ", {, }, |, ^, `, \.
     * This method assumes that the result will be surrounded with angle brackets (&lt; and &gt;).
     *
     * @param value The absolute URI to escape
     * @return The escaped URI
     */
    public static String escapeAbsoluteURIW3C(String value) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i != value.length(); i++) {
            char c = value.charAt(i);
            if (c <= 0x20 || ESCAPED_GLYPHS_ABSOLUTE_URIS.contains(Character.toString(c))) {
                String s = Integer.toHexString(c);
                while (s.length() < 4)
                    s = "0" + s;
                builder.append("\\u");
                builder.append(s);
            } else
                builder.append(c);
        }
        return builder.toString();
    }

    /**
     * Escapes special characters in the specified string according to the common W3C requirements for Turtle, N-Triples, N-quads, etc.
     * All characters are copied as-is, except for the following, which are escaped with a reverse solidus (\) prefix:
     * ", \ and special control characters \t, \r, \n, \b, \f.
     * This method assumes that the result will be quoted with the double quotes characters (").
     *
     * @param value The value to escape
     * @return The escaped value
     */
    public static String escapeStringW3C(String value) {
        return escapeStringBaseDoubleQuote(value);
    }

    /**
     * Escapes special characters in the specified string according to the CSV requirements
     * (See <a href="http://www.ietf.org/rfc/rfc4180.txt">CSV</a>)
     * All characters are copied as-is, except the double quote ("), which is doubled:
     * input    : output
     * a        : a
     * 'a'      : 'a'
     * "b"c     : ""b""c
     * This method assumes that the result will be quoted with the double quotes characters (").
     *
     * @param value The value to escape
     * @return The escaped value
     */
    public static String escapeStringCSV(String value) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i != value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"')
                builder.append('"');
            builder.append(c);
        }
        return builder.toString();
    }

    /**
     * Escapes special characters in the specified string according to the TSV requirements
     * All characters are copied as-is, except for the following, which are escaped with a reverse solidus (\) prefix:
     * ", \ and special control characters \t, \r, \n, \b, \f.
     * This method assumes that the result will be quoted with the double quotes characters (").
     *
     * @param value The value to escape
     * @return The escaped value
     */
    public static String escapeStringTSV(String value) {
        return escapeStringBaseDoubleQuote(value);
    }

    /**
     * Escapes special characters in the specified string according to the JSON requirements
     * All characters are copied as-is, except for the following, which are escaped with a reverse solidus (\) prefix:
     * ", \ and special control characters \t, \r, \n, \b, \f.
     * This method assumes that the result will be quoted with the double quotes characters (").
     *
     * @param value The value to escape
     * @return The escaped value
     */
    public static String escapeStringJSON(String value) {
        return escapeStringBaseDoubleQuote(value);
    }

    /**
     * Escapes basic special characters in the specified string assuming the result will be quoted with the double quotes characters (")
     * All characters are copied as-is, except for the following, which are escaped with a reverse solidus (\) prefix:
     * ", \ and special control characters \0, \a, \t, \r, \n, \b, \f.
     *
     * @param value The value to escape
     * @return The escaped value
     */
    public static String escapeStringBaseDoubleQuote(String value) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i != value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"')
                builder.append("\\\"");
            else if (c == '\\')
                builder.append("\\\\");
            else if (c == '\u0000')
                builder.append("\\0");
            else if (c == '\u0007')
                builder.append("\\a");
            else if (c == '\t')
                builder.append("\\t");
            else if (c == '\r')
                builder.append("\\r");
            else if (c == '\n')
                builder.append("\\n");
            else if (c == '\b')
                builder.append("\\b");
            else if (c == '\f')
                builder.append("\\f");
            else
                builder.append(c);
        }
        return builder.toString();
    }

    /**
     * Escapes basic special characters in the specified string
     * All characters are copied as-is, except for the following, which are escaped with a reverse solidus (\) prefix:
     * special control characters \0, \a, \t, \r, \n, \b, \f.
     *
     * @param value The value to escape
     * @return The escaped value
     */
    public static String escapeStringSpecials(String value) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i != value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\u0000')
                builder.append("\\0");
            else if (c == '\u0007')
                builder.append("\\a");
            else if (c == '\t')
                builder.append("\\t");
            else if (c == '\r')
                builder.append("\\r");
            else if (c == '\n')
                builder.append("\\n");
            else if (c == '\b')
                builder.append("\\b");
            else if (c == '\f')
                builder.append("\\f");
            else
                builder.append(c);
        }
        return builder.toString();
    }
}
