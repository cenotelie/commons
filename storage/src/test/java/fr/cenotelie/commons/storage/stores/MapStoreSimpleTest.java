/*******************************************************************************
 * Copyright (c) 2017 Association Cénotélie (cenotelie.fr)
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

package fr.cenotelie.commons.storage.stores;

import fr.cenotelie.commons.storage.ThreadSafeStorage;
import fr.cenotelie.commons.storage.memory.InMemoryStore;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Test suite for the simple map store
 *
 * @author Laurent Wouters
 */
public class MapStoreSimpleTest {
    /**
     * Numbers of threads
     */
    private static final int THREAD_COUNT = 8;
    /**
     * The number of entries to insert
     */
    private static final int ENTRIES = 1024;

    /**
     * Tests the map for simple insertions and atomic replace
     */
    @Test
    public void testInserts() throws IOException {
        try (ObjectStore store = new ObjectStoreSimple(new InMemoryStore())) {
            MapStoreSimple map = MapStoreSimple.create(store);
            for (int i = 0; i != ENTRIES; i++) {
                Assert.assertTrue("Failed at " + i, map.tryPut(i, i));
            }
            store.flush();
            for (int i = 0; i != ENTRIES; i++) {
                Assert.assertTrue("Failed at " + i, map.compareAndSet(i, i, i + 1));
            }
            store.flush();
            for (int i = 0; i != ENTRIES; i++) {
                Assert.assertFalse("Failed at " + i, map.compareAndSet(i, i, i + 1));
            }
            for (int i = 0; i != ENTRIES; i++) {
                Assert.assertEquals("Wrong mapping", i + 1, map.get(i));
            }
        }
    }

    /**
     * Tests the map for concurrent insertions and atomic replace
     *
     * @throws IOException When an IO operation fails
     */
    @Test
    public void testConcurrentInserts() throws IOException {
        Collection<Thread> threads = new ArrayList<>();
        final boolean successes[] = new boolean[THREAD_COUNT];
        try (ObjectStore store = new ObjectStoreSimple(new ThreadSafeStorage(new InMemoryStore()))) {
            final MapStoreSimple map = MapStoreSimple.create(store);
            for (int i = 0; i != THREAD_COUNT; i++) {
                final int index = i;
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        boolean success = true;
                        try {
                            for (int i = index; i < ENTRIES; i += THREAD_COUNT) {
                                if (!map.tryPut(i, i)) {
                                    success = false;
                                    break;
                                }
                            }
                            for (int i = index; i < ENTRIES; i += THREAD_COUNT) {
                                if (!map.compareAndSet(i, i, i + 1)) {
                                    success = false;
                                    break;
                                }
                            }
                            for (int i = index; i < ENTRIES; i += THREAD_COUNT) {
                                if (map.compareAndSet(i, i, i + 1)) {
                                    success = false;
                                    break;
                                }
                            }
                        } catch (Throwable exception) {
                            success = false;
                        }
                        successes[index] = success;
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
            store.flush();
            for (int i = 0; i != THREAD_COUNT; i++) {
                Assert.assertTrue(successes[i]);
            }
            for (int i = 0; i != ENTRIES; i++) {
                Assert.assertEquals("Wrong mapping", i + 1, map.get(i));
            }
        }
    }
}
