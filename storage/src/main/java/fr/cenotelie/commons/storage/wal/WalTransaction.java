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

import fr.cenotelie.commons.storage.*;

import java.util.Arrays;
import java.util.Date;

/**
 * Represents a user transaction for a write-ahead log that can be used to perform reading and writing
 * A transaction is expected to be used by one thread only, and is only usable and the thread that created the transaction.
 * A transaction MUST be closed.
 *
 * @author Laurent Wouters
 */
class WalTransaction extends Transaction {
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
     * The virtual storage system to use for providing accesses through this transaction
     */
    private final Storage storage;
    /**
     * The cached pages
     */
    private WalPage[] pages;
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
    public WalTransaction(WriteAheadLog parent, long endMark, boolean writable, boolean autocommit) {
        super(writable, autocommit);
        this.parent = parent;
        this.endMark = endMark;
        this.timestamp = (new Date()).getTime();
        this.storage = new SnapshotStorage();
        this.pages = new WalPage[8];
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

    @Override
    protected void doCommit() throws ConcurrentWriteException {
        LogTransactionData data = getLogData();
        if (data != null)
            parent.doTransactionCommit(data, endMark);
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

    @Override
    protected void onClose() {
        for (int i = 0; i != pagesCount; i++) {
            pages[i].release();
        }
        parent.onTransactionEnd(this);
    }

    @Override
    protected Access newAccess(long index, int length, boolean writable) {
        WalAccess access = parent.acquireAccess();
        access.init(storage, index, length, this.writable & writable);
        return access;
    }

    /**
     * Acquires a page of the storage system
     *
     * @param location The location of the page to acquire
     * @return The acquired page
     */
    private WalPage acquirePage(long location) {
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

    /**
     * Represents a virtual storage system to use for providing snapshot access through this (isolated) transaction
     */
    private class SnapshotStorage extends Storage {
        @Override
        public boolean isWritable() {
            return WalTransaction.this.writable;
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
            return WalTransaction.this.acquirePage(index);
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
}
