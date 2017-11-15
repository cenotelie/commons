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
import fr.cenotelie.commons.storage.files.RawFile;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements a write-ahead log that guard the access to another backend storage system
 * A write-ahead log provides the following guarantees:
 * - Transactions are Atomic
 * - Transactions are Isolated (with snapshot isolation)
 * - Transactions are Durable
 *
 * @author Laurent Wouters
 */
public class WriteAheadLog implements AutoCloseable {
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
    public WriteAheadLog(RawFile backend, RawFile log) throws IOException {
        this.backend = backend;
        this.log = log;
        this.sequencer = new AtomicLong(0);
        this.lastCommitted = new AtomicLong(-1);
        reload();
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
        try (StorageAccess access = new StorageAccess(backend, 0, (int) size, false)) {
            while (access.getIndex() < size) {
                try {
                    // load the data for this transaction
                    TransactionData data = new TransactionData(access, false);
                    // apply to the backend storage
                    data.applyTo(backend);
                } catch (IndexOutOfBoundsException exception) {
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
     * @param writable Whether the transaction shall support writing
     * @return The new transaction
     */
    public Transaction newTransaction(boolean writable) {
        return null;
    }

    /**
     * When the transaction ended, commit the edits (if any) to the log
     *
     * @param transaction The transaction that ended
     * @throws ConcurrentWriting when a concurrent transaction already committed conflicting changes to the log
     */
    void onTransactionEnd(Transaction transaction) throws ConcurrentWriting {

    }

    /**
     * Acquires a page of the backend storage system
     *
     * @param endMark  The sequence number of the last transaction known to the current one
     * @param location The location in the backend storage system of the requested page
     * @param writable Whether the page shall be writable
     * @return The requested page
     */
    Page acquirePage(long endMark, long location, boolean writable) {
        return null;
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
    public void close() throws Exception {
        //
    }
}
