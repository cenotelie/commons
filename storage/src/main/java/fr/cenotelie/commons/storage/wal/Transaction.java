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

package fr.cenotelie.commons.storage.wal;

import fr.cenotelie.commons.storage.InMemoryStore;
import fr.cenotelie.commons.storage.StorageAccess;

import java.util.Arrays;

/**
 * Represents a user transaction for a write-ahead log that can be used to perform reading and writing
 *
 * @author Laurent Wouters
 */
public class Transaction implements AutoCloseable {
    /**
     * The parent write-ahead log
     */
    private final WriteAheadLog parent;
    /**
     * The sequence number for this transaction
     */
    final long sequenceNumber;
    /**
     * The sequence number of the last transaction known to this one
     */
    final long endMark;
    /**
     * Whether this transaction allows writing
     */
    private final boolean writable;
    /**
     * The location of the touched pages
     */
    private long[] pageLocations;
    /**
     * The cached pages
     */
    private Page[] pages;
    /**
     * The number of cached pages
     */
    private int pagesCount;

    /**
     * Initializes this transaction
     *
     * @param parent         The parent write-ahead log
     * @param sequenceNumber The sequence number for this transaction
     * @param endMark        The sequence number of the last transaction known to this one
     * @param writable       Whether this transaction allows writing
     */
    Transaction(WriteAheadLog parent, long sequenceNumber, long endMark, boolean writable) {
        this.parent = parent;
        this.sequenceNumber = sequenceNumber;
        this.endMark = endMark;
        this.writable = writable;
        this.pageLocations = new long[8];
        this.pages = new Page[8];
        this.pagesCount = 0;
    }

    /**
     * Closes this transaction and commit the edits (if any) made within this transaction to the write-ahead log
     *
     * @throws ConcurrentWriting when a concurrent transaction already committed conflicting changes to the log
     */
    @Override
    public void close() throws ConcurrentWriting {
        parent.onTransactionEnd(this);
    }

    /**
     * Gets whether this transaction allows writing to the backend storage system
     *
     * @return Whether this transaction allows writing to the backend storage system
     */
    public boolean isWritable() {
        return writable;
    }

    /**
     * Accesses the content of the backend storage system through an access element
     * An access must be within the boundaries of a page.
     *
     * @param index    The index within this file of the reserved area for the access
     * @param length   The length of the reserved area for the access
     * @param writable Whether the access shall allow writing
     * @return The access element
     */
    public StorageAccess access(long index, int length, boolean writable) {
        writable = this.writable & writable;
        Page page = acquirePage(index, writable);
        return new StorageAccess(page, index, length, writable);
    }

    /**
     * Acquires a page of the backend storage system
     *
     * @param location The location of the page to acquire
     * @param writable Whether the page shall be writable
     * @return The acquired page
     */
    private Page acquirePage(long location, boolean writable) {
        location = location & (~InMemoryStore.INDEX_MASK_LOWER);
        for (int i = 0; i != pagesCount; i++) {
            if (pageLocations[i] == location) {
                if (!writable || pages[i].isWritable())
                    return pages[i];
                // swap for a writable page
                pages[i] = parent.acquirePage(endMark, location, true);
                return pages[i];
            }
        }
        // not in the cache
        if (pagesCount >= pages.length) {
            pageLocations = Arrays.copyOf(pageLocations, pageLocations.length * 2);
            pages = Arrays.copyOf(pages, pages.length * 2);
        }
        pageLocations[pagesCount] = location;
        pages[pagesCount] = parent.acquirePage(endMark, location, writable);
        return pages[pagesCount++];
    }
}
