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

import fr.cenotelie.commons.storage.StorageAccess;
import fr.cenotelie.commons.storage.StorageBackend;
import fr.cenotelie.commons.utils.logging.Logging;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements a write-ahead log that guard the access to another backend storage system
 * A write-ahead log provides the following guarantees:
 * - Atomicity, transactions are either fully committed to the log, or not
 * - Isolation, transactions only see the changes of transactions fully committed before they started using snapshot isolation
 * - Durability, transactions are fully written to the log and flushed to disk before returning as committed
 *
 * @author Laurent Wouters
 */
public class WriteAheadLog implements AutoCloseable {
    /**
     * The size of the pool for pages
     */
    private static final int POOL_PAGES_SIZE = 1024;
    /**
     * The size of the pool for accesses
     */
    private static final int POOL_ACCESSES_SIZE = 1024;
    /**
     * The size of the buffer for concurrently running transactions
     */
    private static final int TRANSACTIONS_BUFFER = 16;
    /**
     * The size of the index
     */
    private static final int INDEX_SIZE = 1024;

    /**
     * The backend is now closed
     */
    private static final int STATE_CLOSED = -1;
    /**
     * The backend is ready for IO
     */
    private static final int STATE_READY = 0;
    /**
     * State flag for locking access to the transactions register
     */
    private static final int STATE_FLAG_TRANSACTIONS_LOCK = 1;
    /**
     * State flag for locking access to the index register
     */
    private static final int STATE_FLAG_INDEX_LOCK = 2;
    /**
     * The flag for locking access to the backend for writing
     */
    private static final int STATE_FLAG_BACKEND_LOCK = 4;


    /**
     * The backend storage that is guarded by this WAL
     */
    private final StorageBackend backend;
    /**
     * The storage for the log itself
     */
    private final StorageBackend log;
    /**
     * The current state of the log
     */
    private final AtomicInteger state;
    /**
     * The pool of pages
     */
    private final Page[] pages;
    /**
     * The pool of accesses
     */
    private final TransactionAccess[] accesses;
    /**
     * The currently running transactions
     */
    private volatile Transaction[] transactions;
    /**
     * The number of running transactions
     */
    private volatile int transactionsCount;
    /**
     * The index of transaction data currently in the log
     */
    private volatile LogTransactionData[] index;
    /**
     * The number of transaction data in the index
     */
    private volatile int indexLength;
    /**
     * The identifier of the last committed transaction
     */
    private volatile long indexLastCommitted;
    /**
     * The next identifier for transactions
     */
    private final AtomicLong indexSequencer;

    /**
     * Initializes this log
     *
     * @param backend The backend storage that is guarded by this WAL
     * @param log     The storage for the log itself
     * @throws IOException when an error occurred while accessing storage
     */
    public WriteAheadLog(StorageBackend backend, StorageBackend log) throws IOException {
        this.backend = backend;
        this.log = log;
        this.state = new AtomicInteger(STATE_CLOSED);
        reload();
        this.state.set(STATE_READY);
        this.pages = new Page[POOL_PAGES_SIZE];
        this.accesses = new TransactionAccess[POOL_ACCESSES_SIZE];
        for (int i = 0; i != POOL_PAGES_SIZE; i++)
            this.pages[i] = new Page();
        for (int i = 0; i != POOL_ACCESSES_SIZE; i++)
            this.accesses[i] = new TransactionAccess();
        this.transactions = new Transaction[TRANSACTIONS_BUFFER];
        this.transactionsCount = 0;
        this.index = new LogTransactionData[INDEX_SIZE];
        this.indexLength = 0;
        this.indexLastCommitted = -1;
        this.indexSequencer = new AtomicLong(0);
    }

    /**
     * Reloads this log
     *
     * @throws IOException when an error occurred while accessing storage
     */
    private void reload() throws IOException {
        long size = log.getSize();
        if (size == 0)
            // nothing to do
            return;
        DateFormat dateFormat = DateFormat.getDateTimeInstance();
        try (StorageAccess access = new StorageAccess(backend, 0, (int) size, false)) {
            while (access.getIndex() < size) {
                try {
                    // load the data for this transaction
                    LogTransactionData data = new LogTransactionData(access);
                    Logging.get().warning("WAL: Recovered transaction " + data.getSequenceNumber() + ", started at " + dateFormat.format(new Date(data.getTimestamp())));
                    // apply to the backend storage
                    data.applyTo(backend);
                    Logging.get().warning("WAL: Applied transaction " + data.getSequenceNumber());
                } catch (IndexOutOfBoundsException exception) {
                    Logging.get().warning("WAL: Ended with a partial (unrecoverable) transaction");
                    // stop here
                    break;
                }
            }
        }
        backend.flush();
        // truncate the log
        log.truncate(0);
        log.flush();
    }

    /**
     * Locks a resource in this log
     *
     * @param flag The flag used for locking the resource
     */
    private void stateLock(int flag) {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new Error("Log is closed");
            if ((s & flag) != 0)
                // flag is already used
                continue;
            if (state.compareAndSet(s, s | flag))
                break;
        }
    }

    /**
     * Releases the lock on a resource in this log
     *
     * @param flag The flag used for locking the resource
     */
    private void stateRelease(int flag) {
        while (true) {
            int s = state.get();
            int target = s & (~flag);
            if (state.compareAndSet(s, target))
                break;
        }
    }

    /**
     * Locks the backend for writing back
     */
    private void stateLockBackendWriting() {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new Error("Log is closed");
            if ((s & STATE_FLAG_BACKEND_LOCK) == STATE_FLAG_BACKEND_LOCK)
                // another thread is already writing
                continue;
            if ((s & 0x0000FF00) > 0)
                // some threads are reading
                continue;
            if (state.compareAndSet(s, s | STATE_FLAG_BACKEND_LOCK))
                break;
        }
    }

    /**
     * Releases the lock in the backend for writing
     */
    private void stateReleaseBackendWriting() {
        while (true) {
            int s = state.get();
            int target = s & (~STATE_FLAG_BACKEND_LOCK);
            if (state.compareAndSet(s, target))
                break;
        }
    }

    /**
     * Begins a reading access to the backend
     */
    private void stateBeginBackendReading() {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new Error("Log is closed");
            int count = (s & 0x0000FF00) >>> 8;
            if (count == 0xFF)
                // already 255 reading threads
                continue;
            int target = (s & 0xFFFF00FF) | ((count + 1) << 8);
            if (state.compareAndSet(s, target))
                break;
        }
    }

    /**
     * Ends a reading access to the backend
     */
    private void stateEndBackendReading() {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new Error("Log is closed");
            int count = (s & 0x0000FF00) >>> 8;
            int target = (s & 0xFFFF00FF) | ((count - 1) << 8);
            if (state.compareAndSet(s, target))
                break;
        }
    }

    /**
     * Starts a new transaction
     * The transaction must be ended by a call to the transaction's close method.
     * The transaction will NOT automatically commit when closed, the commit method should be called before closing.
     *
     * @param writable Whether the transaction shall support writing
     * @return The new transaction
     */
    public Transaction newTransaction(boolean writable) {
        return newTransaction(writable, false);
    }

    /**
     * Starts a new transaction
     * The transaction must be ended by a call to the transaction's close method.
     *
     * @param writable   Whether the transaction shall support writing
     * @param autocommit Whether this transaction should commit when being closed
     * @return The new transaction
     */
    public Transaction newTransaction(boolean writable, boolean autocommit) {
        stateLock(STATE_FLAG_TRANSACTIONS_LOCK);
        try {
            Transaction transaction = new Transaction(this, indexLastCommitted, writable, autocommit);
            // register this transaction
            if (transactionsCount >= transactions.length) {
                transactions = Arrays.copyOf(transactions, transactions.length * 2);
                transactions[transactionsCount] = transaction;
            } else {
                for (int i = 0; i != transactions.length; i++) {
                    if (transactions[i] == null) {
                        transactions[i] = transaction;
                        break;
                    }
                }
            }
            transactionsCount++;
            return transaction;
        } finally {
            stateRelease(STATE_FLAG_TRANSACTIONS_LOCK);
        }
    }

    /**
     * Gets the next free sequence number
     *
     * @return The next free sequence number
     */
    long getSequenceNumber() {
        return indexSequencer.getAndIncrement();
    }

    /**
     * Commits the data of a transaction to a log
     *
     * @param data    The data of the transaction to commit
     * @param endMark The end mark for this transaction
     * @throws ConcurrentWriting when a concurrent transaction already committed conflicting changes to the log
     */
    void doTransactionCommit(LogTransactionData data, long endMark) throws ConcurrentWriting {
        stateLock(STATE_FLAG_INDEX_LOCK);
        try {
            if (indexLastCommitted > endMark) {
                // check for concurrent writing from unknown transactions
                for (int i = 0; i != indexLength; i++) {
                    if (index[i].getSequenceNumber() > endMark) {
                        // this transaction is NOT known to the committing one (after the end-mark)
                        // examine this transaction for concurrent edits
                        if (data.intersects(index[i]))
                            throw new ConcurrentWriting(index[i].getSequenceNumber(), index[i].getTimestamp());
                    }
                }
            }
            // no conflict, write this transaction to the log
            data.logLocation = log.getSize();
            try (StorageAccess access = log.access(data.logLocation, data.getLength(), true)) {
                data.writeTo(access);
            }
            try {
                log.flush();
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
            // adds to the index
            if (indexLength == index.length)
                index = Arrays.copyOf(index, index.length * 2);
            index[indexLength++] = data;
            indexLastCommitted = data.getSequenceNumber();
        } finally {
            stateRelease(STATE_FLAG_INDEX_LOCK);
        }
    }

    /**
     * When the transaction ended
     * Unregisters this transaction
     *
     * @param transaction The transaction that ended
     */
    void onTransactionEnd(Transaction transaction) {
        stateLock(STATE_FLAG_TRANSACTIONS_LOCK);
        try {
            for (int i = 0; i != transactions.length; i++) {
                if (transactions[i] == transaction) {
                    transactions[i] = null;
                    transactionsCount--;
                    break;
                }
            }
        } finally {
            stateRelease(STATE_FLAG_TRANSACTIONS_LOCK);
        }
    }

    /**
     * Acquires a page of the backend storage system
     *
     * @param location The location in the backend storage system of the requested page
     * @param endMark  The sequence number of the last transaction known to the current one
     * @return The requested page
     */
    Page acquirePage(long location, long endMark) {
        if (state.get() == STATE_CLOSED)
            throw new Error("Log is closed");
        for (int i = 0; i != POOL_PAGES_SIZE; i++) {
            if (pages[i].reserve()) {
                // reserved a free page
                loadPage(pages[i], location, endMark);
                return pages[i];
            }
        }
        // no free page?
        // create a new page
        Page page = new Page();
        page.reserve();
        loadPage(page, location, endMark);
        return page;
    }

    /**
     * Loads and initializes a page at the specified location up to the specified end mark
     *
     * @param page     The page to load and initialize
     * @param location The page's location in the backend
     * @param endMark  The sequence number of the last transaction known to the current one
     */
    private void loadPage(Page page, long location, long endMark) {
        stateBeginBackendReading();
        try {
            page.loadBase(backend, location);
        } finally {
            stateEndBackendReading();
        }
        stateLock(STATE_FLAG_INDEX_LOCK);
        try {
            for (int i = 0; i != indexLength; i++) {
                if (index[i].getSequenceNumber() > endMark)
                    // this transaction is NOT known, stop here
                    break;
                // look for a matching page in this transaction
                LogPageData data = index[i].getPage(location);
                if (data == null)
                    continue;
                try (StorageAccess access = log.access(index[i].logLocation + data.offset, data.getSerializationLength(), false)) {
                    page.loadEdits(access, data);
                }
            }
            page.makeReady(location);
        } finally {
            stateRelease(STATE_FLAG_INDEX_LOCK);
        }
    }

    /**
     * Acquires an access for a transaction
     *
     * @return An access to be initialized
     */
    TransactionAccess acquireAccess() {
        if (state.get() == STATE_CLOSED)
            throw new Error("Log is closed");
        for (int i = 0; i != POOL_ACCESSES_SIZE; i++) {
            if (accesses[i].reserve())
                return accesses[i];
        }
        // no candidate for reuse
        return new TransactionAccess();
    }

    /**
     * Executes a checkpoint
     */
    private void checkpoint() {
        stateLock(STATE_FLAG_TRANSACTIONS_LOCK | STATE_FLAG_INDEX_LOCK);
        try {
            doCheckpoint();
        } finally {
            stateRelease(STATE_FLAG_TRANSACTIONS_LOCK | STATE_FLAG_INDEX_LOCK);
        }
    }

    /**
     * Executes a checkpoint
     */
    private void doCheckpoint() {
        // find the lowest end mark for the running transactions
        long minEndMark = Long.MAX_VALUE;
        for (int i = 0; i != transactionsCount; i++) {
            minEndMark = Math.min(minEndMark, transactions[i].getEndMark());
        }
        // find the index of the last first transaction in the log that cannot be committed
        // because there is a running transaction that does not know it
        int first = -1;
        for (int i = 0; i != indexLength; i++) {
            if (index[i].getSequenceNumber() > minEndMark) {
                first = i;
                break;
            }
        }
        if (first <= 0)
            // nothing to do
            return;
        // TODO: apply the stored transactions
    }

    @Override
    public void close() throws IOException {
        stateLock(STATE_FLAG_TRANSACTIONS_LOCK | STATE_FLAG_INDEX_LOCK);
        try {
            if (transactionsCount > 0)
                Logging.get().warning("WAL: " + transactionsCount + " transaction(s) still running will be aborted.");
            doCheckpoint();
            backend.close();
            log.close();
        } finally {
            state.set(STATE_CLOSED);
        }
    }
}
