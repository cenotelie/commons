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

import java.util.Arrays;
import java.util.Iterator;

/**
 * Tests for the concatenated iterator
 */
public class ConcatenatedIteratorTest {
    private static final Integer[] content0 = new Integer[]{
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9
    };
    private static final Integer[] content1 = new Integer[]{
            10, 11, 12, 13, 14, 15, 16, 17, 18, 19
    };
    private static final Integer[] content2 = new Integer[]{
            20, 21, 22, 23, 24, 25, 26, 27, 28, 29
    };

    @Test
    public void test_1seq_size() {
        Iterator<Integer> iterator = new ConcatenatedIterator<>(new Iterator[]{
                Arrays.asList(content0).iterator()
        });
        int index = 0;
        while (iterator.hasNext()) {
            iterator.next();
            index++;
        }
        Assert.assertEquals(content0.length, index);
    }

    @Test
    public void test_1seq_content() {
        Iterator<Integer> iterator = new ConcatenatedIterator<>(new Iterator[]{
                Arrays.asList(content0).iterator()
        });
        int index = 0;
        while (iterator.hasNext()) {
            Assert.assertEquals(content0[index], iterator.next());
            index++;
        }
        Assert.assertEquals(content0.length, index);
    }

    @Test
    public void test_2seq_size() {
        Iterator<Integer> iterator = new ConcatenatedIterator<>(new Iterator[]{
                Arrays.asList(content0).iterator(),
                Arrays.asList(content1).iterator()
        });
        int index = 0;
        while (iterator.hasNext()) {
            iterator.next();
            index++;
        }
        Assert.assertEquals(content0.length + content1.length, index);
    }

    @Test
    public void test_2seq_content() {
        Iterator<Integer> iterator = new ConcatenatedIterator<>(new Iterator[]{
                Arrays.asList(content0).iterator(),
                Arrays.asList(content1).iterator()
        });
        int index = 0;
        while (iterator.hasNext()) {
            if (index < content0.length)
                Assert.assertEquals(content0[index], iterator.next());
            else
                Assert.assertEquals(content1[index - content0.length], iterator.next());
            index++;
        }
        Assert.assertEquals(content0.length + content1.length, index);
    }

    @Test
    public void test_3seq_size() {
        Iterator<Integer> iterator = new ConcatenatedIterator<>(new Iterator[]{
                Arrays.asList(content0).iterator(),
                Arrays.asList(content1).iterator(),
                Arrays.asList(content2).iterator()
        });
        int index = 0;
        while (iterator.hasNext()) {
            iterator.next();
            index++;
        }
        Assert.assertEquals(content0.length + content1.length + content2.length, index);
    }

    @Test
    public void test_3seq_content() {
        Iterator<Integer> iterator = new ConcatenatedIterator<>(new Iterator[]{
                Arrays.asList(content0).iterator(),
                Arrays.asList(content1).iterator(),
                Arrays.asList(content2).iterator()
        });
        int index = 0;
        while (iterator.hasNext()) {
            if (index < content0.length)
                Assert.assertEquals(content0[index], iterator.next());
            else if (index - content0.length < content1.length)
                Assert.assertEquals(content1[index - content0.length], iterator.next());
            else
                Assert.assertEquals(content2[index - content0.length - content1.length], iterator.next());
            index++;
        }
        Assert.assertEquals(content0.length + content1.length + content2.length, index);
    }
}
