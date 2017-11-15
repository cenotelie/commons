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
     * When the transaction ended, commit the edits (if any) to the log
     *
     * @param transaction The transaction that ended
     */
    void onTransactionEnd(Transaction transaction) {

    }

    /**
     * Acquires a page of the backend storage system
     *
     * @param endMark  The sequence number of the last transaction known to the current one
     * @param location The location in the backend storage system of the requested page
     * @return The requested page
     */
    Page acquirePage(long endMark, long location) {
        return null;
    }

    /**
     * When a page is no longer required
     *
     * @param page The page that is being released
     */
    void onReleasePage(Page page) {

    }

    /**
     * Acquires an access for a transaction
     *
     * @return An access to be initialized
     */
    TransactionAccess acquireAccess() {
        return null;
    }

    /**
     * When an access for a transaction has ended
     *
     * @param access The access that ended
     */
    void onAccessEnd(TransactionAccess access) {

    }

    @Override
    public void close() throws IOException {
        //
    }
}
