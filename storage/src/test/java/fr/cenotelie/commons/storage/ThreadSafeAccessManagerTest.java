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

package fr.cenotelie.commons.storage;

import fr.cenotelie.commons.storage.memory.InMemoryStore;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

/**
 * Tests for the correct management of the IO accesses
 *
 * @author Laurent Wouters
 */
public class ThreadSafeAccessManagerTest {
    /**
     * Numbers of threads
     */
    private static final int THREAD_COUNT = 16;
    /**
     * The number of entries to insert
     */
    private static final int ACCESSES_COUNT = 1 << 16;


    /**
     * Tests for the management of concurrent accesses
     */
    @Test
    public void testConcurrentAccesses() {
        Collection<Thread> threads = new ArrayList<>();
        final Random random = new Random();
        final ThreadSafeAccessManager manager = new ThreadSafeAccessManager(new InMemoryStore());

        final boolean failures[] = new boolean[THREAD_COUNT];
        for (int i = 0; i != THREAD_COUNT; i++) {
            final int index = i;
            failures[i] = false;
            Thread thread = new Thread(() -> {
                for (int j = 0; j != ACCESSES_COUNT; j++) {
                    int location = random.nextInt() & 0xFFFF;
                    int length = random.nextInt() & 0x00FF;
                    if (length == 0)
                        length = 1;
                    try (Access access = manager.get(location, length, false)) {
                        if (location != access.getLocation() || length != access.getLength()) {
                            failures[index] = true;
                            return;
                        }
                    }
                }
            }, "Test Thread " + i);
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException exception) {
                exception.printStackTrace();
            }
        }
        for (int i = 0; i != THREAD_COUNT; i++)
            Assert.assertFalse(failures[i]);
    }
}
