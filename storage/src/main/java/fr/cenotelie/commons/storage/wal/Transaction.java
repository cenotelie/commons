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

import fr.cenotelie.commons.storage.Constants;
import fr.cenotelie.commons.storage.StorageAccess;
import fr.cenotelie.commons.storage.StorageBackend;
import fr.cenotelie.commons.storage.StorageEndpoint;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

/**
 * Represents a user transaction for a write-ahead log that can be used to perform reading and writing
 * A transaction is expected to be used by one thread only.
 * A transaction MUST be closed
 *
 * @author Laurent Wouters
 */
public class Transaction implements AutoCloseable {
    /**
     * This transaction is currently running
     */
    public static final int STATE_RUNNING = 0;
    /**
     * This transaction has been aborted
     */
    public static final int STATE_ABORTED = 1;
    /**
     * This transaction is currently being committed to the log
     */
    public static final int STATE_COMMITTING = 2;
    /**
     * This transaction has been successfully committed
     */
    public static final int STATE_COMMITTED = 3;
    /**
     * This transaction has been rejected by the log (probably due to concurrent writing)
     */
    public static final int STATE_REJECTED = 4;

    /**
     * Represents a backend to use for providing access through this transaction
     */
    private class Backend extends StorageBackend {
        @Override
        public boolean isWritable() {
            return Transaction.this.writable;
        }

        @Override
        public long getSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean truncate(long length) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void flush() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageEndpoint acquireEndpointAt(long index) {
            return Transaction.this.acquirePage(index);
        }

        @Override
        public void releaseEndpoint(StorageEndpoint endpoint) {
            // do nothing here, the pages are returned at the end of the transaction
        }

        @Override
        public void close() throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * The parent write-ahead log
     */
    private final WriteAheadLog parent;
    /**
     * The sequence number for this transaction
     */
    private final long sequenceNumber;
    /**
     * The sequence number of the last transaction known to this one
     */
    private final long endMark;
    /**
     * The timestamp for this transaction
     */
    private final long timestamp;
    /**
     * Whether this transaction allows writing
     */
    private final boolean writable;
    /**
     * Whether this transaction should commit when being closed
     */
    private final boolean autocommit;
    /**
     * The backend to use for providing access through this transaction
     */
    private final StorageBackend backend;
    /**
     * The current state of this transaction
     */
    private int state;
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
     * @param autocommit     Whether this transaction should commit when being closed
     */
    Transaction(WriteAheadLog parent, long sequenceNumber, long endMark, boolean writable, boolean autocommit) {
        this.parent = parent;
        this.sequenceNumber = sequenceNumber;
        this.endMark = endMark;
        this.timestamp = (new Date()).getTime();
        this.writable = writable;
        this.autocommit = autocommit;
        this.backend = new Backend();
        this.state = STATE_RUNNING;
        this.pages = new Page[8];
        this.pagesCount = 0;
    }

    /**
     * Gets the sequence number of this transaction
     *
     * @return The sequence number of this transaction
     */
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * Gets the sequence number of the last transaction known to this one
     *
     * @return The sequence number of the last transaction known to this one
     */
    public long getEndMark() {
        return endMark;
    }

    /**
     * Gets the timestamp of this transaction
     *
     * @return The timestamp of this transaction
     */
    public long getTimestamp() {
        return timestamp;
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
     * Gets whether this transaction should commit when being closed
     *
     * @return Whether this transaction should commit when being closed
     */
    public boolean isAutocommit() {
        return autocommit;
    }

    /**
     * Gets the current state of this transaction
     *
     * @return The current state of this transaction
     */
    public int getState() {
        return state;
    }

    /**
     * Commits this transaction to the parent log
     *
     * @throws ConcurrentWriting when a concurrent transaction already committed conflicting changes to the log
     */
    public void commit() throws ConcurrentWriting {
        if (state != STATE_RUNNING)
            throw new Error("Bad state");
        state = STATE_COMMITTING;
        try {
            for (int i = 0; i != pagesCount; i++)
                pages[i].compact();
            parent.doTransactionCommit(this);
            state = STATE_COMMITTED;
        } catch (ConcurrentWriting exception) {
            state = STATE_REJECTED;
            throw exception;
        }
    }

    /**
     * Gets the log data for indexing this transaction
     *
     * @return The log data, or null if no page is dirty
     */
    LogTransactionData getLogData() {
        int dirtyPagesCount = 0;
        for (int i = 0; i != pagesCount; i++) {
            if (pages[i].isDirty())
                dirtyPagesCount++;
        }
        if (dirtyPagesCount == 0)
            return null;
        LogTransactionPageData[] pageData = new LogTransactionPageData[dirtyPagesCount];
        int index = 0;
        for (int i = 0; i != pagesCount; i++) {
            if (pages[i].isDirty()) {
                pageData[index++] = pages[i].getLogData();
            }
        }
        return new LogTransactionData(sequenceNumber, timestamp, pageData);
    }

    /**
     * Aborts this transaction
     */
    public void abort() {
        if (state != STATE_RUNNING)
            throw new Error("Bad state");
        state = STATE_ABORTED;
    }

    /**
     * Closes this transaction and commit the edits (if any) made within this transaction to the write-ahead log
     *
     * @throws ConcurrentWriting when a concurrent transaction already committed conflicting changes to the log
     */
    @Override
    public void close() throws ConcurrentWriting {
        if (state == STATE_RUNNING) {
            if (autocommit)
                commit();
            else
                state = STATE_ABORTED; // abort this transaction
        }
        for (int i = 0; i != pagesCount; i++)
            pages[i].release(sequenceNumber);
        parent.onTransactionEnd(this);
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
        if (state != STATE_RUNNING)
            throw new Error("Bad state");
        TransactionAccess access = parent.acquireAccess();
        access.init(backend, index, length, this.writable & writable);
        return access;
    }

    /**
     * Acquires a page of the backend storage system
     *
     * @param location The location of the page to acquire
     * @return The acquired page
     */
    private Page acquirePage(long location) {
        location = location & (~Constants.INDEX_MASK_LOWER);
        for (int i = 0; i != pagesCount; i++) {
            if (pages[i].getLocation() == location)
                return pages[i];
        }
        // not in the cache
        if (pagesCount >= pages.length)
            pages = Arrays.copyOf(pages, pages.length * 2);
        pages[pagesCount] = parent.acquirePage(location, endMark);
        return pages[pagesCount++];
    }
}
