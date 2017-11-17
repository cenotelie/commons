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
     * The backend storage that is guarded by this WAL
     */
    private final StorageBackend backend;
    /**
     * The storage for the log itself
     */
    private final StorageBackend log;
    /**
     * The next identifier for transactions
     */
    private final AtomicLong sequencer;
    /**
     * The identifier of the last committed transaction
     */
    private final AtomicLong lastCommitted;
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
    private final AtomicInteger transactionsCount;
    /**
     * The index of transaction data currently in the log
     */
    private TransactionData[] index;

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
        this.sequencer = new AtomicLong(0);
        this.lastCommitted = new AtomicLong(-1);
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
        this.transactionsCount = new AtomicInteger(0);
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
                    TransactionData data = new TransactionData(access, false);
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
     * Starts a new transaction
     * The transaction must be ended by a call to the transaction's close method.
     *
     * @param writable   Whether the transaction shall support writing
     * @param autocommit Whether this transaction should commit when being closed
     * @return The new transaction
     */
    public Transaction newTransaction(boolean writable, boolean autocommit) {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new Error("Log is closed");
            if (s == STATE_READY && state.compareAndSet(s, STATE_BUSY))
                break;
        }
        Transaction transaction = new Transaction(this, sequencer.getAndIncrement(), lastCommitted.get(), writable, autocommit);
        // register this transaction
        if (transactionsCount.get() >= transactions.length)
            transactions = Arrays.copyOf(transactions, transactions.length * 2);
        transactions[transactionsCount.getAndIncrement()] = transaction;
        // return in a ready state
        state.set(STATE_READY);
        return transaction;
    }

    /**
     * Commits the data of this transaction to the log
     *
     * @param transaction The transaction to commit
     * @throws ConcurrentWriting when a concurrent transaction already committed conflicting changes to the log
     */
    void doTransactionCommit(Transaction transaction) throws ConcurrentWriting {

    }

    /**
     * When the transaction ended
     * Unregisters this transaction
     *
     * @param transaction The transaction that ended
     */
    void onTransactionEnd(Transaction transaction) {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                return;
            if (s == STATE_READY && state.compareAndSet(s, STATE_BUSY))
                break;
        }
        for (int i = 0; i != transactions.length; i++) {
            if (transactions[i] == transaction) {
                transactions[i] = null;
                break;
            }
        }
        transactionsCount.decrementAndGet();
        // return in a ready state
        state.set(STATE_READY);
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
                // this page is reserved
                if (pages[i].getLocation() == location && pages[i].getEndMark() <= endMark && !pages[i].isDirty()) {
                    // it is a candidate for reuse
                    updatePageTo(pages[i], endMark);
                    return pages[i];
                }
                // oops, not a candidate, release this page
                pages[i].release();
            }
        }
        // no candidate for reuse
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
        page.loadBase(backend, location);
        // TODO: apply edits in the log
        page.makeReady(location, endMark);
    }

    /**
     * Updates a page at the specified location up to the specified end mark
     *
     * @param page    The page to update initialize
     * @param endMark The sequence number of the last transaction known to the current one
     */
    private void updatePageTo(Page page, long endMark) {
        // TODO: apply edits in the log
        page.makeReady(page.getLocation(), endMark);
    }

    /**
     * Acquires an access for a transaction
     *
     * @return An access to be initialized
     */
    TransactionAccess acquireAccess() {
        if (state.get() == STATE_CLOSED)
            throw new Error("Log is closed");
        for (int i = 0; i != POOL_PAGES_SIZE; i++) {
            if (accesses[i].reserve())
                return accesses[i];
        }
        // no candidate for reuse
        return new TransactionAccess();
    }

    @Override
    public void close() throws IOException {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new Error("Log is closed");
            if (s == STATE_READY && state.compareAndSet(s, STATE_BUSY))
                break;
        }
        if (transactionsCount.get() > 0)
            Logging.get().warning("WAL: " + transactionsCount.get() + " transaction(s) still running will be aborted.");
        state.set(STATE_CLOSED);
    }
}
