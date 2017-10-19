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
 * Utility API for Base 64 encoding / decoding
 *
 * @author Laurent Wouters
 */
public class Base64 {
    /**
     * The map for base 64 decoding
     */
    private static final int[] BASE64_MAP = new int[]{
            /*000*/0, 0, 0, 0, 0, 0, 0, 0,
            /*008*/0, 0, 0, 0, 0, 0, 0, 0,
            /*016*/0, 0, 0, 0, 0, 0, 0, 0,
            /*024*/0, 0, 0, 0, 0, 0, 0, 0,
            /*032*/0, 0, 0, 0, 0, 0, 0, 0,
            /*040*/0, 0, 0, 62, 0, 0, 0, 63,
            /*048*/52, 53, 54, 55, 56, 57, 58, 59,
            /*056*/60, 61, 0, 0, 0, 64, 0, 0,
            /*064*/0, 0, 1, 2, 3, 4, 5, 6,
            /*072*/7, 8, 9, 10, 11, 12, 13, 14,
            /*080*/15, 16, 17, 18, 19, 20, 21, 22,
            /*088*/23, 24, 25, 0, 0, 0, 0, 0,
            /*096*/0, 26, 27, 28, 29, 30, 31, 32,
            /*104*/33, 34, 35, 36, 37, 38, 39, 40,
            /*112*/41, 42, 43, 44, 45, 46, 47, 48,
            /*120*/49, 50, 51, 0, 0, 0, 0, 0
    };
    /**
     * The characters for the base64 encoding
     */
    private static final char[] BASE64_CHARS = new char[]{
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            '+', '/', '='};

    /**
     * Decodes a base64 string as a string
     *
     * @param input The input string
     * @return The decoded string
     */
    public static String decodeBase64AsString(String input) {
        return new String(decodeBase64(input), IOUtils.CHARSET);
    }

    /**
     * Decodes a base64 string as a byte buffer
     *
     * @param input The input string
     * @return The decoded bytes
     */
    public static byte[] decodeBase64(String input) {
        if (input == null)
            return null;
        if (input.isEmpty())
            return new byte[0];

        char[] chars = input.toCharArray();
        int padding = (chars[chars.length - 1] == '=' ? 1 : 0);
        if (chars.length >= 2 && chars[chars.length - 2] == '=')
            padding++;

        int length = ((chars.length * 3) / 4) - padding;
        byte result[] = new byte[length];
        int b0, b1, b2, b3;
        int index = 0;
        for (int i = 0; i < chars.length; i += 4) {
            b0 = BASE64_MAP[chars[i]];
            b1 = BASE64_MAP[chars[i + 1]];
            b2 = BASE64_MAP[chars[i + 2]];
            b3 = BASE64_MAP[chars[i + 3]];
            result[index++] = (byte) ((b0 << 2) | (b1 >>> 4));
            if (b2 < 64) {
                result[index++] = (byte) ((b1 << 4) | (b2 >>> 2));
                if (b3 < 64) {
                    result[index++] = (byte) ((b2 << 6) | b3);
                }
            }
        }
        return result;
    }

    /**
     * Encodes a string in base64
     *
     * @param input The input string
     * @return The encoded string
     */
    public static String encodeBase64(String input) {
        return encodeBase64(input.getBytes(IOUtils.CHARSET));
    }

    /**
     * Encodes a byte buffer in base64
     *
     * @param bytes The input bytes
     * @return The encoded string
     */
    public static String encodeBase64(byte[] bytes) {
        if (bytes == null)
            return null;
        if (bytes.length == 0)
            return "";
        char[] chars = new char[bytes.length % 3 == 0 ? bytes.length / 3 * 4 : (bytes.length - bytes.length % 3 + 3) / 3 * 4];
        int target = 0;
        for (int i = 0; i < bytes.length; i += 3) {
            int offset = (bytes[i] & 0xFC) >>> 2;
            chars[target++] = BASE64_CHARS[offset];
            offset = (bytes[i] & 0x03) << 4;
            if (i + 1 < bytes.length) {
                offset |= (bytes[i + 1] & 0xF0) >>> 4;
                chars[target++] = BASE64_CHARS[offset];
                offset = (bytes[i + 1] & 0x0F) << 2;
                if (i + 2 < bytes.length) {
                    offset |= (bytes[i + 2] & 0xC0) >>> 6;
                    chars[target++] = BASE64_CHARS[offset];
                    offset = bytes[i + 2] & 0x3F;
                    chars[target++] = BASE64_CHARS[offset];
                } else {
                    chars[target++] = BASE64_CHARS[offset];
                    chars[target++] = '=';
                }
            } else {
                chars[target++] = BASE64_CHARS[offset];
                chars[target++] = '=';
                chars[target++] = '=';
            }
        }
        return new String(chars);
    }
}
