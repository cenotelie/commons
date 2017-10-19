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

package fr.cenotelie.commons.utils.collections;

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

/**
 * Tests for the single iterator
 */
public class SingleIteratorTest {

    @Test
    public void test_single() {
        Integer item = 0;
        Iterator<Integer> iterator = new SingleIterator<>(item);
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(item, iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void test_empty() {
        Iterator<Integer> iterator = new SingleIterator<>(null);
        Assert.assertFalse(iterator.hasNext());
    }
}
