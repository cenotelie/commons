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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for the combining iterator
 */
public class CombiningIteratorTest {
    private static final Integer[] content = new Integer[]{
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9
    };

    @Test
    public void test_0x0_size() {
        Iterator<Couple<Integer, Integer>> iterator = new CombiningIterator<>(
                new SingleIterator<>(null),
                element -> new SingleIterator<>(null));

        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        Assert.assertEquals(0, count);
    }

    @Test
    public void test_0x1_size() {
        Iterator<Couple<Integer, Integer>> iterator = new CombiningIterator<>(
                new SingleIterator<>(null),
                element -> new SingleIterator<>(0));

        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        Assert.assertEquals(0, count);
    }

    @Test
    public void test_0xn_size() {
        Iterator<Couple<Integer, Integer>> iterator = new CombiningIterator<>(
                new SingleIterator<>(null),
                element -> Arrays.asList(content).iterator());

        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        Assert.assertEquals(0, count);
    }

    @Test
    public void test_1x0_size() {
        Iterator<Couple<Integer, Integer>> iterator = new CombiningIterator<>(
                new SingleIterator<>(0),
                element -> new SingleIterator<>(null));

        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        Assert.assertEquals(0, count);
    }

    @Test
    public void test_1x1_size() {
        Iterator<Couple<Integer, Integer>> iterator = new CombiningIterator<>(
                new SingleIterator<>(0),
                element -> new SingleIterator<>(0));

        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        Assert.assertEquals(1, count);
    }

    @Test
    public void test_1xn_size() {
        Iterator<Couple<Integer, Integer>> iterator = new CombiningIterator<>(
                new SingleIterator<>(0),
                element -> Arrays.asList(content).iterator());

        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        Assert.assertEquals(content.length, count);
    }

    @Test
    public void test_nx0_size() {
        Iterator<Couple<Integer, Integer>> iterator = new CombiningIterator<>(
                Arrays.asList(content).iterator(),
                element -> new SingleIterator<>(null));

        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        Assert.assertEquals(0, count);
    }

    @Test
    public void test_nx1_size() {
        Iterator<Couple<Integer, Integer>> iterator = new CombiningIterator<>(
                Arrays.asList(content).iterator(),
                element -> new SingleIterator<>(0));

        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        Assert.assertEquals(content.length, count);
    }

    @Test
    public void test_nxn_size() {
        Iterator<Couple<Integer, Integer>> iterator = new CombiningIterator<>(
                Arrays.asList(content).iterator(),
                element -> Arrays.asList(content).iterator());

        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        Assert.assertEquals(content.length * content.length, count);
    }

    @Test
    public void test_1x1_content() {
        Iterator<Couple<Integer, Integer>> iterator = new CombiningIterator<>(
                new SingleIterator<>(0),
                element -> new SingleIterator<>(0));

        while (iterator.hasNext()) {
            Couple<Integer, Integer> couple = iterator.next();
            Assert.assertEquals((Integer) 0, couple.x);
            Assert.assertEquals((Integer) 0, couple.y);
        }
    }

    @Test
    public void test_1xn_content() {
        Iterator<Couple<Integer, Integer>> iterator = new CombiningIterator<>(
                new SingleIterator<>(0),
                element -> Arrays.asList(content).iterator());

        int index = 0;
        while (iterator.hasNext()) {
            Couple<Integer, Integer> couple = iterator.next();
            Assert.assertEquals((Integer) 0, couple.x);
            Assert.assertEquals((Integer) index, couple.y);
            index++;
        }
    }

    @Test
    public void test_nx1_content() {
        Iterator<Couple<Integer, Integer>> iterator = new CombiningIterator<>(
                Arrays.asList(content).iterator(),
                element -> new SingleIterator<>(0));

        int index = 0;
        while (iterator.hasNext()) {
            Couple<Integer, Integer> couple = iterator.next();
            Assert.assertEquals((Integer) index, couple.x);
            Assert.assertEquals((Integer) 0, couple.y);
            index++;
        }
    }

    @Test
    public void test_nxn_content() {
        Iterator<Couple<Integer, Integer>> iterator = new CombiningIterator<>(
                Arrays.asList(content).iterator(),
                element -> Arrays.asList(content).iterator());

        int indexX = 0;
        int indexY = 0;
        while (iterator.hasNext()) {
            Couple<Integer, Integer> couple = iterator.next();
            Assert.assertEquals((Integer) indexX, couple.x);
            Assert.assertEquals((Integer) indexY, couple.y);
            indexY++;
            if (indexY >= content.length) {
                indexY = 0;
                indexX++;
            }
        }
    }

    @Test
    public void test_nxn_parameters() {
        final AtomicInteger index = new AtomicInteger(0);
        Iterator<Couple<Integer, Integer>> iterator = new CombiningIterator<>(
                Arrays.asList(content).iterator(),
                element -> {
                    Assert.assertEquals(content[index.getAndIncrement()], element);
                    return Arrays.asList(content).iterator();
                });

        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        Assert.assertEquals(content.length * content.length, count);
    }
}
