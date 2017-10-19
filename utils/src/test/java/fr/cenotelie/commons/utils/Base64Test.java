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

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for IOUtils
 *
 * @author Laurent Wouters
 */
public class Base64Test {

    @Test
    public void testEncodeBase64() {
        Assert.assertEquals("YW55IGNhcm5hbCBwbGVhc3VyZS4=", Base64.encodeBase64("any carnal pleasure."));
        Assert.assertEquals("YW55IGNhcm5hbCBwbGVhc3VyZQ==", Base64.encodeBase64("any carnal pleasure"));
        Assert.assertEquals("YW55IGNhcm5hbCBwbGVhc3Vy", Base64.encodeBase64("any carnal pleasur"));
        Assert.assertEquals("YW55IGNhcm5hbCBwbGVhc3U=", Base64.encodeBase64("any carnal pleasu"));
        Assert.assertEquals("YW55IGNhcm5hbCBwbGVhcw==", Base64.encodeBase64("any carnal pleas"));
    }

    @Test
    public void testDecodeBase64() {
        Assert.assertEquals("any carnal pleasure.", Base64.decodeBase64AsString("YW55IGNhcm5hbCBwbGVhc3VyZS4="));
        Assert.assertEquals("any carnal pleasure", Base64.decodeBase64AsString("YW55IGNhcm5hbCBwbGVhc3VyZQ=="));
        Assert.assertEquals("any carnal pleasur", Base64.decodeBase64AsString("YW55IGNhcm5hbCBwbGVhc3Vy"));
        Assert.assertEquals("any carnal pleasu", Base64.decodeBase64AsString("YW55IGNhcm5hbCBwbGVhc3U="));
        Assert.assertEquals("any carnal pleas", Base64.decodeBase64AsString("YW55IGNhcm5hbCBwbGVhcw=="));
    }
}
