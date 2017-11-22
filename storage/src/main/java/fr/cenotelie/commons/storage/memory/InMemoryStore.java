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
 * Implements an in-memory storage system
 *
 * @author Laurent Wouters
 */
public class InMemoryStore extends Storage {
    /**
     * The storage system is ready for IO
     */
    private static final int STATE_READY = 0;
    /**
     * The storage system is currently busy with an operation
     */
    private static final int STATE_BUSY = 1;
    /**
     * The storage system is now closed
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
    public boolean cut(long from, long to) {
        if (from < 0 || from > to)
            throw new IndexOutOfBoundsException();
        if (from == to)
            // 0-length cut => do nothing
            return false;

        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new IllegalStateException();
            if (state.compareAndSet(STATE_READY, STATE_BUSY))
                break;
        }
        try {
            // do we have to update the current size?
            while (true) {
                long currentSize = size.get();
                if (from >= currentSize)
                    // start after the current size => no effect
                    return false;
                if (to < currentSize)
                    // no effect on the current size ...
                    break;
                if (size.compareAndSet(currentSize, from)) {
                    // only cut up to the end ...
                    to = Math.min(to, currentSize);
                    break;
                }
            }
            // get the page and page-specific indices
            int fromPage = (int) (from >>> Constants.PAGE_INDEX_LENGTH);
            int fromIndex = (int) (from & Constants.INDEX_MASK_LOWER);
            int toPage = (int) (to >>> Constants.PAGE_INDEX_LENGTH);
            int toIndex = (int) (to & Constants.INDEX_MASK_LOWER);
            if (toIndex == 0) {
                toPage--;
                toIndex = Constants.PAGE_SIZE;
            }
            // within the same page
            if (fromPage == toPage) {
                if (pages[fromPage] == null)
                    // nothing to do, but we did cut the empty!
                    return true;
                pages[fromPage].zeroes(fromIndex, toIndex);
                return true;
            }
            // cut the first page
            if (fromIndex == 0)
                // completely cut the page
                pages[fromPage] = null;
            else if (pages[fromPage] != null)
                // do not cut the complete page, but cut the content
                pages[fromPage].zeroes(fromIndex, Constants.PAGE_SIZE);
            for (int i = fromPage + 1; i != toPage; i++) {
                // completely cut intermediate pages
                pages[i] = null;
            }
            // cut the last page
            if (toIndex == Constants.PAGE_SIZE)
                // completely cut the page
                pages[toPage] = null;
            else if (pages[toPage] != null)
                // do not cut the complete page, but cut the content
                pages[toPage].zeroes(0, toIndex);
            return true;
        } finally {
            state.set(STATE_READY);
        }
    }

    @Override
    public boolean extendTo(long length) throws IOException {
        if (length < 0)
            throw new IndexOutOfBoundsException();
        return onWriteUpTo(length);
    }

    @Override
    public void flush() throws IOException {
        // do nothing
    }

    @Override
    public Endpoint acquireEndpointAt(long index) {
        if (index < 0)
            throw new IndexOutOfBoundsException();
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new IllegalStateException();
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
    public void close() {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new IllegalStateException();
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
     * @return Whether the size was modified
     */
    boolean onWriteUpTo(long index) {
        while (true) {
            long current = size.get();
            if (current > index)
                // not the furthest => exit
                return false;
            if (size.compareAndSet(current, index))
                // succeeded to update => exit
                return true;
            // start over
        }
    }
}
