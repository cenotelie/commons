/*******************************************************************************
 * Copyright (c) 2020 Association Cénotélie (cenotelie.fr)
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

public class TextUtilsTest {

    @Test
    public void testEscapeUCP() {
        Assert.assertEquals("A", TextUtils.escapeStringW3C("A"));
        Assert.assertEquals("\\u00B1", TextUtils.escapeStringW3C("±"));
        Assert.assertEquals("\\u20AC", TextUtils.escapeStringW3C("€"));
        Assert.assertEquals("\\U00010437", TextUtils.escapeStringW3C("\uD801\uDC37"));
    }
}
