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
import fr.cenotelie.commons.storage.Endpoint;
import fr.cenotelie.commons.storage.Storage;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements an in-memory storage backend
 *
 * @author Laurent Wouters
 */
public class InMemoryStore extends Storage {
    /**
     * The backend is ready for IO
     */
    private static final int STATE_READY = 0;
    /**
     * The backend is currently busy with an operation
     */
    private static final int STATE_BUSY = 1;
    /**
     * The backend is now closed
     */
    private static final int STATE_CLOSED = -1;


    /**
     * The size of this store
     */
    private final AtomicLong size;
    /**
     * The pages
     */
    private volatile InMemoryPage[] pages;
    /**
     * The current state of this storage
     */
    private final AtomicInteger state;

    /**
     * Initializes this store
     */
    public InMemoryStore() {
        this.size = new AtomicLong(0);
        this.pages = new InMemoryPage[8];
        this.state = new AtomicInteger(STATE_READY);
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
    public boolean truncate(long length) throws IOException {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new IOException("The file is closed");
            if (state.compareAndSet(STATE_READY, STATE_BUSY))
                break;
        }
        try {
            while (true) {
                long currentSize = size.get();
                if (length >= currentSize)
                    return false;
                if (size.compareAndSet(currentSize, length))
                    break;
            }
            int lastPage = (int) (length >>> Constants.PAGE_INDEX_LENGTH);
            int lastIndex = (int) (length & Constants.INDEX_MASK_LOWER);
            if (lastIndex == 0) {
                lastPage--;
                lastIndex = Constants.PAGE_SIZE;
            }
            if (lastPage >= pages.length)
                // too small
                return false;
            for (int i = pages.length - 1; i != lastPage; i--) {
                // drop this page
                pages[i] = null;
            }
            if (lastIndex != Constants.PAGE_SIZE && pages[lastPage] != null)
                pages[lastPage].zeroesFrom(lastIndex);
            return true;
        } finally {
            state.set(STATE_READY);
        }
    }

    @Override
    public void flush() throws IOException {
        // do nothing
    }

    @Override
    public Endpoint acquireEndpointAt(long index) {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new RuntimeException(new IOException("The file is closed"));
            if (state.compareAndSet(STATE_READY, STATE_BUSY))
                break;
        }
        try {
            int requested = (int) (index >>> Constants.PAGE_INDEX_LENGTH);
            while (requested >= pages.length) {
                pages = Arrays.copyOf(pages, pages.length * 2);
            }
            if (pages[requested] == null)
                pages[requested] = new InMemoryPage(this, requested << Constants.PAGE_INDEX_LENGTH);
            return pages[requested];
        } finally {
            state.set(STATE_READY);
        }
    }

    @Override
    public void releaseEndpoint(Endpoint endpoint) {
        // do nothing
    }

    @Override
    public void close() throws IOException {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new IOException("The file is closed");
            if (state.compareAndSet(STATE_READY, STATE_BUSY))
                break;
        }
        try {
            pages = null;
        } finally {
            state.set(STATE_CLOSED);
        }
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
