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

package fr.cenotelie.commons.utils.http;

import fr.cenotelie.commons.utils.IOUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Additional tests for URIUtils
 *
 * @author Laurent Wouters
 */
public class URIUtilsTest {
    /**
     * Tests for the encoding and decoding of URI components
     *
     * @param input   The input string
     * @param encoded The expected encoded string
     */
    private static void testEncodeDecode(String input, String encoded) {
        String value = URIUtils.encodeComponent(input);
        Assert.assertEquals(encoded, value);
        value = URIUtils.decodeComponent(value);
        Assert.assertEquals(input, value);
    }

    /**
     * Tests for the encoding and decoding of URI components
     *
     * @param specialChar The Unicode code point of the special char to test for
     */
    private static void testEncodeDecode(int specialChar) {
        String string = new String(Character.toChars(specialChar));
        byte[] bytes = string.getBytes(IOUtils.UTF8);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i != bytes.length; i++) {
            builder.append("%");
            int value = ((int) bytes[i]) & 0xFF;
            if (value < 0x10)
                builder.append("0");
            builder.append(Integer.toString(value, 16));
        }
        String encoded = builder.toString();
        testEncodeDecode(string, encoded);
    }

    @Test
    public void testEncodeDecodeLetters() {
        testEncodeDecode("xxx", "xxx");
    }

    @Test
    public void testEncodeDecodeDigit() {
        testEncodeDecode("x0x", "x0x");
    }

    @Test
    public void testEncodeDecodeDash() {
        testEncodeDecode("x-x", "x-x");
    }

    @Test
    public void testEncodeDecodeUnderscore() {
        testEncodeDecode("x_x", "x_x");
    }

    @Test
    public void testEncodeDecodeDot() {
        testEncodeDecode("x.x", "x.x");
    }

    @Test
    public void testEncodeDecodeBang() {
        testEncodeDecode("x!x", "x!x");
    }

    @Test
    public void testEncodeDecodeTilde() {
        testEncodeDecode("x~x", "x~x");
    }

    @Test
    public void testEncodeDecodeStar() {
        testEncodeDecode("x*x", "x*x");
    }

    @Test
    public void testEncodeDecodeQuote() {
        testEncodeDecode("x'x", "x'x");
    }

    @Test
    public void testEncodeDecodeLeftParenthesis() {
        testEncodeDecode("x(x", "x(x");
    }

    @Test
    public void testEncodeDecodeRightParenthesis() {
        testEncodeDecode("x)x", "x)x");
    }

    @Test
    public void testEncodeDecodeSpace() {
        testEncodeDecode("x x", "x%20x");
    }

    @Test
    public void testEncodeDecode1Byte() {
        testEncodeDecode(0x007F);
    }

    @Test
    public void testEncodeDecode2BytesLeftBound() {
        testEncodeDecode(0x0080);
    }

    @Test
    public void testEncodeDecode2BytesRightBound() {
        testEncodeDecode(0x07FF);
    }

    @Test
    public void testEncodeDecode3BytesLeftBound() {
        testEncodeDecode(0x0800);
    }

    @Test
    public void testEncodeDecode3BytesRightBound() {
        testEncodeDecode(0xFFFF);
    }

    @Test
    public void testEncodeDecode4BytesLeftBound() {
        testEncodeDecode(0x10000);
    }

    @Test
    public void testEncodeDecode4BytesRightBound() {
        testEncodeDecode(0x10FFFF);
    }
}
