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

package fr.cenotelie.commons.storage.memory;

import fr.cenotelie.commons.storage.Constants;
import fr.cenotelie.commons.storage.StorageBackend;
import fr.cenotelie.commons.storage.StorageEndpoint;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements an in-memory storage backend
 *
 * @author Laurent Wouters
 */
public class InMemoryStore extends StorageBackend {
    /**
     * The size of this store
     */
    private final AtomicLong size;
    /**
     * The pages
     */
    private volatile InMemoryPage[] pages;

    /**
     * Initializes this store
     */
    public InMemoryStore() {
        this.size = new AtomicLong(0);
        this.pages = new InMemoryPage[8];
    }

    @Override
    public boolean isWritable() {
        return true;
    }

    @Override
    public long getSize() {
        return size.get();
    }

    @Override
    public void flush() throws IOException {
        // do nothing
    }

    @Override
    public synchronized StorageEndpoint acquireEndpointAt(long index) {
        int requested = (int) (index >>> Constants.PAGE_INDEX_LENGTH);
        while (requested >= pages.length) {
            pages = Arrays.copyOf(pages, pages.length * 2);
        }
        if (pages[requested] == null)
            pages[requested] = new InMemoryPage(this, requested << Constants.PAGE_INDEX_LENGTH);
        return pages[requested];
    }

    @Override
    public void releaseEndpoint(StorageEndpoint endpoint) {
        // do nothing
    }

    @Override
    public void close() {
        pages = null;
    }

    /**
     * When a thread wrote up to an index
     *
     * @param index The maximum index written to
     */
    void onWriteUpTo(long index) {
        while (true) {
            long current = size.get();
            if (current > index)
                // not the furthest => exit
                return;
            if (size.compareAndSet(current, index))
                // succeeded to update => exit
                return;
            // start over
        }
    }
}
