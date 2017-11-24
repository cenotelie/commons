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

import fr.cenotelie.commons.storage.Access;
import fr.cenotelie.commons.storage.Constants;
import fr.cenotelie.commons.storage.Endpoint;
import fr.cenotelie.commons.storage.Storage;

import java.util.Arrays;
import java.util.Date;

/**
 * Represents a user transaction for a write-ahead log that can be used to perform reading and writing
 * A transaction is expected to be used by one thread only, and is only usable and the thread that created the transaction.
 * A transaction MUST be closed.
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
     * Represents a virtual storage system to use for providing snapshot access through this (isolated) transaction
     */
    private class SnapshotStorage extends Storage {
        @Override
        public boolean isWritable() {
            return Transaction.this.writable;
        }

        @Override
        public long getSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean cut(long from, long to) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void flush() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Endpoint acquireEndpointAt(long index) {
            return Transaction.this.acquirePage(index);
        }

        @Override
        public void releaseEndpoint(Endpoint endpoint) {
            // do nothing here, the pages are returned at the end of the transaction
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * The thread that created and is running this transaction
     */
    private final Thread thread;
    /**
     * The parent write-ahead log
     */
    private final WriteAheadLog parent;
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
     * The virtual storage system to use for providing accesses through this transaction
     */
    private final Storage storage;
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
     * The sequence number attributed to this transaction
     */
    private long sequenceNumber;

    /**
     * Initializes this transaction
     *
     * @param parent     The parent write-ahead log
     * @param endMark    The sequence number of the last transaction known to this one
     * @param writable   Whether this transaction allows writing
     * @param autocommit Whether this transaction should commit when being closed
     */
    Transaction(WriteAheadLog parent, long endMark, boolean writable, boolean autocommit) {
        this.thread = Thread.currentThread();
        this.parent = parent;
        this.endMark = endMark;
        this.timestamp = (new Date()).getTime();
        this.writable = writable;
        this.autocommit = autocommit;
        this.storage = new SnapshotStorage();
        this.state = STATE_RUNNING;
        this.pages = new Page[8];
        this.pagesCount = 0;
        this.sequenceNumber = -1;
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
     * Gets whether this transaction allows writing to the storage system
     *
     * @return Whether this transaction allows writing to the storage system
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
     * Gets whether this transaction is an orphan because the thread that created it is no longer alive and the transaction is still running
     *
     * @return Whether this transaction is orphan
     */
    public boolean isOrphan() {
        return state == STATE_RUNNING && !thread.isAlive();
    }

    /**
     * Commits this transaction to the parent log
     *
     * @throws ConcurrentWritingException when a concurrent transaction already committed conflicting changes to the log
     */
    public void commit() throws ConcurrentWritingException {
        if (thread != Thread.currentThread() && thread.isAlive())
            throw new WrongThreadException();
        if (state != STATE_RUNNING)
            throw new IllegalStateException();
        state = STATE_COMMITTING;
        try {
            LogTransactionData data = getLogData();
            if (data != null)
                parent.doTransactionCommit(data, endMark);
            state = STATE_COMMITTED;
        } catch (ConcurrentWritingException exception) {
            // the commit is rejected
            state = STATE_REJECTED;
            throw exception;
        } catch (Throwable throwable) {
            // any other exception => abort
            state = STATE_ABORTED;
            throw throwable;
        }
    }

    /**
     * Gets the log data for indexing this transaction
     *
     * @return The log data, or null if no page is dirty
     */
    private LogTransactionData getLogData() {
        int dirtyPagesCount = 0;
        for (int i = 0; i != pagesCount; i++) {
            if (pages[i].isDirty())
                dirtyPagesCount++;
        }
        if (dirtyPagesCount == 0)
            return null;
        LogPageData[] pageData = new LogPageData[dirtyPagesCount];
        sequenceNumber = parent.getSequenceNumber();
        int index = 0;
        int offset = LogTransactionData.SERIALIZATION_SIZE_HEADER; // seq number, timestamp and pages count
        for (int i = 0; i != pagesCount; i++) {
            if (pages[i].isDirty()) {
                pageData[index] = pages[i].getLogData(offset);
                offset += pageData[i].getSerializationLength();
                index++;
            }
        }
        return new LogTransactionData(sequenceNumber, timestamp, pageData);
    }

    /**
     * Aborts this transaction
     */
    public void abort() {
        if (thread != Thread.currentThread() && thread.isAlive())
            throw new WrongThreadException();
        if (state != STATE_RUNNING)
            throw new IllegalStateException();
        state = STATE_ABORTED;
    }

    /**
     * Closes this transaction and commit the edits (if any) made within this transaction to the write-ahead log
     *
     * @throws ConcurrentWritingException when a concurrent transaction already committed conflicting changes to the log
     */
    @Override
    public void close() throws ConcurrentWritingException {
        if (thread != Thread.currentThread() && thread.isAlive())
            throw new WrongThreadException();
        if (state == STATE_RUNNING) {
            if (autocommit)
                commit();
            else
                state = STATE_ABORTED; // abort this transaction
        }
        for (int i = 0; i != pagesCount; i++) {
            pages[i].release();
        }
        parent.onTransactionEnd(this);
    }

    /**
     * Accesses the content of the storage system through an access element
     * An access must be within the boundaries of a page.
     *
     * @param index    The index within this file of the reserved area for the access
     * @param length   The length of the reserved area for the access
     * @param writable Whether the access shall allow writing
     * @return The access element
     */
    public Access access(long index, int length, boolean writable) {
        if (thread != Thread.currentThread() && thread.isAlive())
            throw new WrongThreadException();
        if (index < 0 || length <= 0)
            throw new IllegalArgumentException();
        if (state != STATE_RUNNING)
            throw new IllegalStateException();
        TransactionAccess access = parent.acquireAccess();
        access.init(storage, index, length, this.writable & writable);
        return access;
    }

    /**
     * Acquires a page of the storage system
     *
     * @param location The location of the page to acquire
     * @return The acquired page
     */
    private Page acquirePage(long location) {
        if (thread != Thread.currentThread() && thread.isAlive())
            throw new WrongThreadException();
        location = location & Constants.INDEX_MASK_UPPER;
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
