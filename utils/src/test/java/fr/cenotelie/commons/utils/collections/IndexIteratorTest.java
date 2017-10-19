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
 * Tests for the index iterator
 */
public class IndexIteratorTest {
    private static final Integer[] contentFull = new Integer[]{
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9
    };

    private static final Integer[] content0 = new Integer[]{
            null, 1, 2, 3, 4, 5, 6, 7, 8, 9
    };

    private static final Integer[] content9 = new Integer[]{
            0, 1, 2, 3, 4, 5, 6, 7, 8, null
    };

    private static final Integer[] content27 = new Integer[]{
            0, 1, null, 3, 4, 5, 6, null, 8, 9
    };

    private static final Integer[] content0239 = new Integer[]{
            null, 1, null, null, 4, 5, 6, 7, 8, null
    };

    @Test
    public void test_full_sequence_size() {
        Iterator<Integer> iterator = new IndexIterator<>(contentFull);
        int index = 0;
        while (iterator.hasNext()) {
            iterator.next();
            index++;
        }
        Assert.assertEquals(contentFull.length, index);
    }

    @Test
    public void test_full_sequence_content() {
        Iterator<Integer> iterator = new IndexIterator<>(contentFull);
        int index = 0;
        while (iterator.hasNext()) {
            Assert.assertEquals((Integer) index, iterator.next());
            index++;
        }
        Assert.assertEquals(contentFull.length, index);
    }

    @Test
    public void test_skip_first_size() {
        Iterator<Integer> iterator = new IndexIterator<>(content0);
        int index = 0;
        while (iterator.hasNext()) {
            iterator.next();
            index++;
        }
        Assert.assertEquals(content0.length - 1, index);
    }

    @Test
    public void test_skip_first_content() {
        Iterator<Integer> iterator = new IndexIterator<>(content0);
        int index = 0;
        while (iterator.hasNext()) {
            Assert.assertEquals((Integer) (index + 1), iterator.next());
            index++;
        }
        Assert.assertEquals(content0.length - 1, index);
    }

    @Test
    public void test_skip_last_size() {
        Iterator<Integer> iterator = new IndexIterator<>(content9);
        int index = 0;
        while (iterator.hasNext()) {
            iterator.next();
            index++;
        }
        Assert.assertEquals(content9.length - 1, index);
    }

    @Test
    public void test_skip_last_content() {
        Iterator<Integer> iterator = new IndexIterator<>(content9);
        int index = 0;
        while (iterator.hasNext()) {
            Assert.assertEquals((Integer) index, iterator.next());
            index++;
        }
        Assert.assertEquals(content9.length - 1, index);
    }

    @Test
    public void test_skip_any_size() {
        Iterator<Integer> iterator = new IndexIterator<>(content27);
        int index = 0;
        while (iterator.hasNext()) {
            iterator.next();
            index++;
        }
        Assert.assertEquals(content27.length - 2, index);
    }

    @Test
    public void test_skip_any_content() {
        Iterator<Integer> iterator = new IndexIterator<>(content27);
        int index = 0;
        while (iterator.hasNext()) {
            while (content27[index] == null)
                index++;
            Assert.assertEquals((Integer) index, iterator.next());
            index++;
        }
        Assert.assertEquals(content27.length, index);
    }

    @Test
    public void test_skip_double_size() {
        Iterator<Integer> iterator = new IndexIterator<>(content0239);
        int index = 0;
        while (iterator.hasNext()) {
            iterator.next();
            index++;
        }
        Assert.assertEquals(content0239.length - 4, index);
    }

    @Test
    public void test_skip_double_content() {
        Iterator<Integer> iterator = new IndexIterator<>(content0239);
        int index = 0;
        while (iterator.hasNext()) {
            while (content0239[index] == null)
                index++;
            Assert.assertEquals((Integer) index, iterator.next());
            index++;
        }
        Assert.assertEquals(content0239.length - 1, index);
    }
}
