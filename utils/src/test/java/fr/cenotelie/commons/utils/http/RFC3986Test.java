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

import org.junit.Assert;
import org.junit.Test;

/**
 * Test suite for the implementation of RFC 3986
 *
 * @author Laurent Wouters
 */
public class RFC3986Test {
    /**
     * The base URI to use
     */
    private static final String BASE = "http://a/b/c/d;p?q";

    /**
     * Tests a positive result of the relative resolution algorithm
     *
     * @param input    The input URI
     * @param expected The expected output
     */
    private static void testRelativeResolution(String input, String expected) {
        String output = URIUtils.resolveRelative(BASE, input);
        Assert.assertEquals(expected, output);
    }

    @Test
    public void testRelativeResolution001() {
        testRelativeResolution("g:h", "g:h");
    }

    @Test
    public void testRelativeResolution002() {
        testRelativeResolution("g", "http://a/b/c/g");
    }

    @Test
    public void testRelativeResolution003() {
        testRelativeResolution("./g", "http://a/b/c/g");
    }

    @Test
    public void testRelativeResolution004() {
        testRelativeResolution("g/", "http://a/b/c/g/");
    }

    @Test
    public void testRelativeResolution005() {
        testRelativeResolution("/g", "http://a/g");
    }

    @Test
    public void testRelativeResolution006() {
        testRelativeResolution("//g", "http://g");
    }

    @Test
    public void testRelativeResolution007() {
        testRelativeResolution("?y", "http://a/b/c/d;p?y");
    }

    @Test
    public void testRelativeResolution008() {
        testRelativeResolution("g?y", "http://a/b/c/g?y");
    }

    @Test
    public void testRelativeResolution009() {
        testRelativeResolution("#s", "http://a/b/c/d;p?q#s");
    }

    @Test
    public void testRelativeResolution010() {
        testRelativeResolution("g#s", "http://a/b/c/g#s");
    }

    @Test
    public void testRelativeResolution011() {
        testRelativeResolution("g?y#s", "http://a/b/c/g?y#s");
    }

    @Test
    public void testRelativeResolution012() {
        testRelativeResolution(";x", "http://a/b/c/;x");
    }

    @Test
    public void testRelativeResolution013() {
        testRelativeResolution("g;x", "http://a/b/c/g;x");
    }

    @Test
    public void testRelativeResolution014() {
        testRelativeResolution("g;x?y#s", "http://a/b/c/g;x?y#s");
    }

    @Test
    public void testRelativeResolution015() {
        testRelativeResolution("", "http://a/b/c/d;p?q");
    }

    @Test
    public void testRelativeResolution016() {
        testRelativeResolution(".", "http://a/b/c/");
    }

    @Test
    public void testRelativeResolution017() {
        testRelativeResolution("./", "http://a/b/c/");
    }

    @Test
    public void testRelativeResolution018() {
        testRelativeResolution("..", "http://a/b/");
    }

    @Test
    public void testRelativeResolution019() {
        testRelativeResolution("../", "http://a/b/");
    }

    @Test
    public void testRelativeResolution020() {
        testRelativeResolution("../g", "http://a/b/g");
    }

    @Test
    public void testRelativeResolution021() {
        testRelativeResolution("../..", "http://a/");
    }

    @Test
    public void testRelativeResolution022() {
        testRelativeResolution("../../", "http://a/");
    }

    @Test
    public void testRelativeResolution023() {
        testRelativeResolution("../../g", "http://a/g");
    }

    @Test
    public void testRelativeResolution024() {
        testRelativeResolution("../../../g", "http://a/g");
    }

    @Test
    public void testRelativeResolution025() {
        testRelativeResolution("../../../../g", "http://a/g");
    }

    @Test
    public void testRelativeResolution026() {
        testRelativeResolution("/./g", "http://a/g");
    }

    @Test
    public void testRelativeResolution027() {
        testRelativeResolution("/../g", "http://a/g");
    }

    @Test
    public void testRelativeResolution028() {
        testRelativeResolution("g.", "http://a/b/c/g.");
    }

    @Test
    public void testRelativeResolution029() {
        testRelativeResolution(".g", "http://a/b/c/.g");
    }

    @Test
    public void testRelativeResolution030() {
        testRelativeResolution("g..", "http://a/b/c/g..");
    }

    @Test
    public void testRelativeResolution031() {
        testRelativeResolution("..g", "http://a/b/c/..g");
    }

    @Test
    public void testRelativeResolution032() {
        testRelativeResolution("./../g", "http://a/b/g");
    }

    @Test
    public void testRelativeResolution033() {
        testRelativeResolution("./g/.", "http://a/b/c/g/");
    }

    @Test
    public void testRelativeResolution034() {
        testRelativeResolution("g/./h", "http://a/b/c/g/h");
    }

    @Test
    public void testRelativeResolution035() {
        testRelativeResolution("g/../h", "http://a/b/c/h");
    }

    @Test
    public void testRelativeResolution036() {
        testRelativeResolution("g;x=1/./y", "http://a/b/c/g;x=1/y");
    }

    @Test
    public void testRelativeResolution037() {
        testRelativeResolution("g;x=1/../y", "http://a/b/c/y");
    }

    @Test
    public void testRelativeResolution038() {
        testRelativeResolution("g?y/./x", "http://a/b/c/g?y/./x");
    }

    @Test
    public void testRelativeResolution039() {
        testRelativeResolution("g?y/../x", "http://a/b/c/g?y/../x");
    }

    @Test
    public void testRelativeResolution040() {
        testRelativeResolution("g#s/./x", "http://a/b/c/g#s/./x");
    }

    @Test
    public void testRelativeResolution041() {
        testRelativeResolution("g#s/../x", "http://a/b/c/g#s/../x");
    }
}
