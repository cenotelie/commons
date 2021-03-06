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
import fr.cenotelie.commons.utils.concurrent.DaemonTaskScheduler;
import fr.cenotelie.commons.utils.logging.Logging;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements a write-ahead log that guard the access to a storage system
 * A write-ahead log provides the following guarantees:
 * - Atomicity, transactions are either fully committed to the log, or not
 * - Isolation, transactions only see the changes of transactions fully committed before they started (snapshot isolation)
 * - Durability, transactions are fully written to the log and flushed to disk before returning as committed
 *
 * @author Laurent Wouters
 */
public class WriteAheadLog extends TransactionalStorage {
    /**
     * The size of a header for the log:
     * - int64: magic number
     * - int64: timestamp for the last header update
     * - int64: number of stored transaction data
     * - int64: location of the first transaction data
     */
    private static final int LOG_HEADER_SIZE = 8 + 8 + 8 + 8;
    /**
     * The magic number for the log's header
     */
    private static final long LOG_HEADER_MAGIC_NUMBER = 0x0063656e2d77616cL; // cen-wal

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
     * The size of the index that should trigger a checkpoint
     */
    private static final int INDEX_TRIGGER = 512;
    /**
     * The size of the index
     */
    private static final int INDEX_BUFFER = INDEX_TRIGGER * 2;
    /**
     * The size of the log that should trigger a checkpoint
     */
    private static final long LOG_SIZE_TRIGGER = 1 << 30; // 1Gb
    /**
     * The wait period for the janitor
     */
    private static final int JANITOR_PERIOD = 5000; // 5 seconds

    /**
     * The storage system is now closed
     */
    private static final int STATE_CLOSED = -1;
    /**
     * The storage system is ready for IO
     */
    private static final int STATE_READY = 0;
    /**
     * State flag for locking the log due to its closing
     * In this state, new transactions cannot be created.
     * Ongoing transactions must terminate.
     */
    private static final int STATE_FLAG_CLOSING_LOCK = 1;
    /**
     * State flag for locking access to the transactions register
     */
    private static final int STATE_FLAG_TRANSACTIONS_LOCK = 2;
    /**
     * State flag for locking access to the index register
     */
    private static final int STATE_FLAG_INDEX_LOCK = 4;
    /**
     * The flag for locking access to the storage system for writing
     */
    private static final int STATE_FLAG_STORAGE_WRITE_LOCK = 8;


    /**
     * The storage system that is guarded by this WAL
     */
    private final Storage storage;
    /**
     * The storage for the log itself
     */
    private final Storage log;
    /**
     * The current state of the log
     */
    private final AtomicInteger state;
    /**
     * The pool of pages
     */
    private final WalPage[] pages;
    /**
     * The pool of accesses
     */
    private final WalAccess[] accesses;
    /**
     * The currently running transactions by thread
     */
    private final WeakHashMap<Thread, Transaction> transactionsByThread;
    /**
     * The next identifier for transactions
     */
    private final AtomicLong indexSequencer;
    /**
     * The scheduler for the janitor's task
     */
    private final DaemonTaskScheduler janitorScheduler;
    /**
     * The currently running transactions
     */
    private volatile WalTransaction[] transactions;
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
     * Initializes this log with a janitor
     *
     * @param storage The storage system that is guarded by this WAL
     * @param log     The storage for the log itself
     * @throws IOException when an error occurred while accessing storage
     */
    public WriteAheadLog(Storage storage, Storage log) throws IOException {
        this(storage, log, true);
    }

    /**
     * Initializes this log
     *
     * @param storage    The storage system that is guarded by this WAL
     * @param log        The storage for the log itself
     * @param useJanitor Whether to use an asynchronous janitor thread
     * @throws IOException when an error occurred while accessing storage
     */
    public WriteAheadLog(Storage storage, Storage log, boolean useJanitor) throws IOException {
        this.storage = storage;
        this.log = log;
        this.state = new AtomicInteger(STATE_CLOSED);
        reload();
        this.state.set(STATE_READY);
        this.pages = new WalPage[POOL_PAGES_SIZE];
        this.accesses = new WalAccess[POOL_ACCESSES_SIZE];
        for (int i = 0; i != POOL_PAGES_SIZE; i++)
            this.pages[i] = new WalPage();
        for (int i = 0; i != POOL_ACCESSES_SIZE; i++)
            this.accesses[i] = new WalAccess();
        this.transactions = new WalTransaction[TRANSACTIONS_BUFFER];
        this.transactionsByThread = new WeakHashMap<>();
        this.transactionsCount = 0;
        this.index = new LogTransactionData[INDEX_BUFFER];
        this.indexLength = 0;
        this.indexLastCommitted = -1;
        this.indexSequencer = new AtomicLong(0);
        if (useJanitor)
            this.janitorScheduler = new DaemonTaskScheduler(this::cleanup, JANITOR_PERIOD);
        else
            this.janitorScheduler = null;
    }

    /**
     * Reloads this log
     *
     * @throws IOException when an error occurred while accessing storage
     */
    private void reload() throws IOException {
        long size = log.getSize();
        if (size <= LOG_HEADER_SIZE)
            // nothing to do
            return;
        DateFormat dateFormat = DateFormat.getDateTimeInstance();

        // try to read the header
        long beginsAt;
        try (Access access = new Access(log, 0, LOG_HEADER_SIZE, false)) {
            if (access.readLong() != LOG_HEADER_MAGIC_NUMBER) {
                // this is not a log
                throw new IOException("The provided storage is not empty and is no a log storage (wrong magic number)");
            }
            long timestamp = access.readLong();
            beginsAt = access.skip(8).readLong(); // skip the number transactions, do not rely on this info, we just read the content
            Logging.get().info("WAL: Reading log with last checkpoint at " + dateFormat.format(new Date(timestamp)));
        }

        if (beginsAt == 0) {
            Logging.get().info("WAL: Nothing to restore.");
            log.truncate(LOG_HEADER_SIZE);
            log.flush();
            return;
        }

        // read and restore
        try (Access access = new Access(log, beginsAt, (int) (size - beginsAt), false)) {
            while (access.getIndex() < size) {
                try {
                    // load the data for this transaction
                    LogTransactionData data = new LogTransactionData(access);
                    Logging.get().warning("WAL: Recovered transaction " + data.getSequenceNumber() + ", committed at " + dateFormat.format(new Date(data.getTimestamp())));
                    // apply to the storage system
                    data.applyTo(storage);
                    Logging.get().warning("WAL: Applied transaction " + data.getSequenceNumber());
                } catch (IndexOutOfBoundsException exception) {
                    Logging.get().warning("WAL: Ended with a partial (unrecoverable) transaction");
                    // stop here
                    break;
                }
            }
        }
        // make sure everything is committed on the device
        storage.flush();

        // truncate, update and flush the log
        log.truncate(LOG_HEADER_SIZE);
        try (Access access = log.access(0, LOG_HEADER_SIZE, true)) {
            access.writeLong(LOG_HEADER_MAGIC_NUMBER);
            access.writeLong((new Date()).getTime());
            access.writeLong(0); // no transaction
            access.writeLong(0); // begin at 0
        }
        log.flush();
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

    @Override
    public boolean isWritable() {
        return storage.isWritable();
    }

    @Override
    public long getSize() {
        return storage.getSize();
    }

    @Override
    public void flush() {
        cleanup();
    }

    @Override
    public WalTransaction newTransaction(boolean writable, boolean autocommit) {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                // already closed ...
                throw new IllegalStateException();
            if ((s & STATE_FLAG_CLOSING_LOCK) == STATE_FLAG_CLOSING_LOCK)
                // flag is already used, someone is already closing this log ...
                throw new IllegalStateException();
            if ((s & STATE_FLAG_TRANSACTIONS_LOCK) == STATE_FLAG_TRANSACTIONS_LOCK)
                // flag is already used by another thread
                continue;
            if (state.compareAndSet(s, s | STATE_FLAG_TRANSACTIONS_LOCK))
                break;
        }
        try {
            WalTransaction transaction = new WalTransaction(this, indexLastCommitted, writable, autocommit);
            // guarded by state with STATE_FLAG_TRANSACTIONS_LOCK
            // register this transaction
            if (transactionsCount >= transactions.length) {
                //noinspection NonAtomicOperationOnVolatileField
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
            // guarded by state with STATE_FLAG_TRANSACTIONS_LOCK
            //noinspection NonAtomicOperationOnVolatileField
            transactionsCount++;
            transactionsByThread.put(Thread.currentThread(), transaction);
            return transaction;
        } finally {
            stateRelease(STATE_FLAG_TRANSACTIONS_LOCK);
            if (janitorScheduler != null)
                janitorScheduler.resetWait();
        }
    }

    @Override
    public Transaction getTransaction() throws NoTransactionException {
        Transaction result = transactionsByThread.get(Thread.currentThread());
        if (result == null)
            throw new NoTransactionException();
        return result;
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
     * @throws ConcurrentWriteException when a concurrent transaction already committed conflicting changes to the log
     */
    void doTransactionCommit(LogTransactionData data, long endMark) throws ConcurrentWriteException {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                // already closed ...
                throw new IllegalStateException();
            if ((s & STATE_FLAG_INDEX_LOCK) == STATE_FLAG_INDEX_LOCK)
                // flag is already used by another thread
                continue;
            if (state.compareAndSet(s, s | STATE_FLAG_INDEX_LOCK))
                break;
        }
        try {
            if (indexLastCommitted > endMark) {
                // check for concurrent writing from unknown transactions
                for (int i = 0; i != indexLength; i++) {
                    if (index[i].getSequenceNumber() > endMark) {
                        // this transaction is NOT known to the committing one (after the end-mark)
                        // examine this transaction for concurrent edits
                        if (data.intersects(index[i]))
                            throw new ConcurrentWriteException();
                    }
                }
            }
            // no conflict, write this transaction to the log
            data.logLocation = Math.max(LOG_HEADER_SIZE, log.getSize());
            // if this is the first transaction, also write the header
            if (indexLength == 0) {
                try (Access access = log.access(0, LOG_HEADER_SIZE, true)) {
                    access.writeLong(LOG_HEADER_MAGIC_NUMBER);
                    access.writeLong((new Date()).getTime());
                    access.writeLong(1);
                    access.writeLong(LOG_HEADER_SIZE);
                }
            }
            try (Access access = log.access(data.logLocation, data.getSerializationLength(), true)) {
                data.serialize(access);
            }
            try {
                log.flush();
            } catch (IOException exception) {
                throw new ConcurrentWriteException(exception);
            }
            // guarded by state with STATE_FLAG_INDEX_LOCK
            // adds to the index
            if (indexLength == index.length)
                //noinspection NonAtomicOperationOnVolatileField
                index = Arrays.copyOf(index, index.length * 2);
            // guarded by state with STATE_FLAG_INDEX_LOCK
            //noinspection NonAtomicOperationOnVolatileField
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
    void onTransactionEnd(WalTransaction transaction) {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                // already closed ...
                throw new IllegalStateException();
            if ((s & STATE_FLAG_TRANSACTIONS_LOCK) == STATE_FLAG_TRANSACTIONS_LOCK)
                // flag is already used by another thread
                continue;
            if (state.compareAndSet(s, s | STATE_FLAG_TRANSACTIONS_LOCK))
                break;
        }
        try {
            for (int i = 0; i != transactions.length; i++) {
                if (transactions[i] == transaction) {
                    transactions[i] = null;
                    // guarded by state with STATE_FLAG_TRANSACTIONS_LOCK
                    //noinspection NonAtomicOperationOnVolatileField
                    transactionsCount--;
                    transactionsByThread.remove(transaction.getThread());
                    break;
                }
            }
        } finally {
            stateRelease(STATE_FLAG_TRANSACTIONS_LOCK);
        }
    }

    /**
     * Acquires a page of the backing storage system
     *
     * @param location The location in the backing storage system of the requested page
     * @param endMark  The sequence number of the last transaction known to the current one
     * @return The requested page
     */
    WalPage acquirePage(long location, long endMark) {
        for (int i = 0; i != POOL_PAGES_SIZE; i++) {
            if (pages[i].reserve()) {
                // reserved a free page
                try {
                    loadPage(pages[i], location, endMark);
                    return pages[i];
                } catch (Throwable throwable) {
                    pages[i].release();
                    throw throwable;
                }
            }
        }
        // no free page?
        // create a new page
        WalPage page = new WalPage();
        page.reserve();
        loadPage(page, location, endMark);
        return page;
    }

    /**
     * Loads and initializes a page at the specified location up to the specified end mark
     *
     * @param page     The page to load and initialize
     * @param location The page's location in the storage system
     * @param endMark  The sequence number of the last transaction known to the current one
     */
    private void loadPage(WalPage page, long location, long endMark) {
        // increase the number of readers
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                // already closed ...
                throw new IllegalStateException();
            int count = (s & 0x0000FF00) >>> 8;
            if (count == 0xFF)
                // already 255 reading threads
                continue;
            int target = (s & 0xFFFF00FF) | ((count + 1) << 8);
            if (state.compareAndSet(s, target))
                break;
        }
        try {
            page.loadBase(storage, location);
        } finally {
            // decrease the number of readers
            while (true) {
                int s = state.get();
                int count = (s & 0x0000FF00) >>> 8;
                int target = (s & 0xFFFF00FF) | ((count - 1) << 8);
                if (state.compareAndSet(s, target))
                    break;
            }
        }
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                // already closed ...
                throw new IllegalStateException();
            if ((s & STATE_FLAG_INDEX_LOCK) == STATE_FLAG_INDEX_LOCK)
                // flag is already used by another thread
                continue;
            if (state.compareAndSet(s, s | STATE_FLAG_INDEX_LOCK))
                break;
        }
        try {
            for (int i = 0; i != indexLength; i++) {
                if (index[i].getSequenceNumber() > endMark)
                    // this transaction is NOT known, stop here
                    break;
                // look for a matching page in this transaction
                LogPageData data = index[i].getPage(location);
                if (data == null)
                    continue;
                try (Access access = log.access(index[i].logLocation + data.offset, data.getSerializationLength(), false)) {
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
    WalAccess acquireAccess() {
        for (int i = 0; i != POOL_ACCESSES_SIZE; i++) {
            if (accesses[i].reserve())
                return accesses[i];
        }
        // no candidate for reuse
        return new WalAccess();
    }

    /**
     * Executes a checkpoint
     *
     * @throws IOException when an error occurred while accessing storage
     */
    private synchronized void checkpointExecute() throws IOException {
        long minEndMark = checkpointGetLowestEndMark();
        while (true) {
            int s = state.get();
            if ((s & STATE_FLAG_INDEX_LOCK) == STATE_FLAG_INDEX_LOCK)
                // flag is already used by another thread
                continue;
            if (state.compareAndSet(s, s | STATE_FLAG_INDEX_LOCK))
                break;
        }
        try {
            if (indexLength == 0)
                // nothing to do
                return;
            int firstUnmovable = indexLength; // the first transaction data that cannot be written back
            for (int i = 0; i != indexLength; i++) {
                if (index[i].getSequenceNumber() >= minEndMark) {
                    // found it
                    firstUnmovable = i;
                    break;
                }
                // write back this transaction
                checkpointWriteBack(index[i]);
            }
            if (firstUnmovable == 0)
                // we did nothing => exit
                return;
            // commit the storage
            storage.flush();
            // here we are reasonably sure that the data is written back to the storage
            // cut the log
            if (firstUnmovable == indexLength) {
                // we committed all transactions, truncate, update and flush the log
                log.truncate(LOG_HEADER_SIZE);
                try (Access access = log.access(0, LOG_HEADER_SIZE, true)) {
                    access.writeLong(LOG_HEADER_MAGIC_NUMBER);
                    access.writeLong((new Date()).getTime());
                    access.writeLong(0); // no transaction
                    access.writeLong(0); // begin at 0
                }
                log.flush();
                indexLength = 0;
                Arrays.fill(index, null);
            } else {
                // cut the content
                log.cut(LOG_HEADER_SIZE, index[firstUnmovable].logLocation);
                // rewrite the log header
                try (Access access = log.access(0, LOG_HEADER_SIZE, true)) {
                    access.writeLong(LOG_HEADER_MAGIC_NUMBER);
                    access.writeLong((new Date()).getTime());
                    access.writeLong(indexLength - firstUnmovable);
                    access.writeLong(index[firstUnmovable].logLocation);
                }
                log.flush();
                int j = 0;
                Arrays.fill(index, 0, firstUnmovable, null);
                for (int i = firstUnmovable; i != indexLength; i++) {
                    index[j++] = index[i];
                    index[i] = null;
                }
                indexLength = (indexLength - firstUnmovable);
            }
        } finally {
            stateRelease(STATE_FLAG_INDEX_LOCK);
        }
    }

    /**
     * Finds the lowest end mark for the running transactions
     *
     * @return The lower end mark
     */
    private long checkpointGetLowestEndMark() {
        while (true) {
            int s = state.get();
            if ((s & STATE_FLAG_TRANSACTIONS_LOCK) == STATE_FLAG_TRANSACTIONS_LOCK)
                // flag is already used by another thread
                continue;
            if (state.compareAndSet(s, s | STATE_FLAG_TRANSACTIONS_LOCK))
                break;
        }
        try {
            long minEndMark = Long.MAX_VALUE;
            for (int i = 0; i != transactionsCount; i++) {
                minEndMark = Math.min(minEndMark, transactions[i].getEndMark());
            }
            return minEndMark;
        } finally {
            stateRelease(STATE_FLAG_TRANSACTIONS_LOCK);
        }
    }

    /**
     * Write back the data of a transaction to the storage system
     *
     * @param transaction The data of a transaction
     */
    private void checkpointWriteBack(LogTransactionData transaction) {
        try (Access access = log.access(transaction.logLocation, transaction.getSerializationLength(), false)) {
            transaction.loadContent(access);
        }
        // locks the storage for writing
        while (true) {
            int s = state.get();
            if ((s & STATE_FLAG_STORAGE_WRITE_LOCK) == STATE_FLAG_STORAGE_WRITE_LOCK)
                // another thread is already writing
                continue;
            if ((s & 0x0000FF00) > 0)
                // some threads are reading
                continue;
            if (state.compareAndSet(s, s | STATE_FLAG_STORAGE_WRITE_LOCK))
                break;
        }
        try {
            transaction.applyTo(storage);
        } finally {
            stateRelease(STATE_FLAG_STORAGE_WRITE_LOCK);
        }
    }

    /**
     * Kills the orphaned transactions
     */
    private void cleanupKillOrphans() {
        // cleanup dead transactions
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                // already closed ...
                throw new IllegalStateException();
            if ((s & STATE_FLAG_TRANSACTIONS_LOCK) == STATE_FLAG_TRANSACTIONS_LOCK)
                // flag is already used by another thread
                continue;
            if (state.compareAndSet(s, s | STATE_FLAG_TRANSACTIONS_LOCK))
                break;
        }
        WalTransaction[] toKill = null;
        int toKillCount = 0;
        try {
            for (int i = 0; i != transactions.length; i++) {
                if (transactions[i] != null && transactions[i].isOrphan()) {
                    if (toKill == null)
                        toKill = new WalTransaction[4];
                    if (toKillCount == toKill.length)
                        toKill = Arrays.copyOf(toKill, toKill.length * 2);
                    toKill[toKillCount++] = transactions[i];
                }
            }
        } finally {
            stateRelease(STATE_FLAG_TRANSACTIONS_LOCK);
        }
        // kill the orphaned transactions
        for (int i = 0; i != toKillCount; i++) {
            transactions[i].abort();
            try {
                transactions[i].close();
            } catch (ConcurrentWriteException exception) {
                // cannot happen because we aborted the transaction before
            }
        }
    }

    /**
     * Manually cleanups this log and trigger a checkpoint if necessary
     * This method will NOT force the execution of a checkpoint.
     */
    public void cleanup() {
        cleanup(false);
    }

    /**
     * Manually cleanups this log and trigger a checkpoint if necessary
     *
     * @param forceCheckpoint Whether to force the execution of a checkpoint
     */
    public void cleanup(boolean forceCheckpoint) {
        cleanupKillOrphans();
        // should we execute a checkpoint?
        if (forceCheckpoint || indexLength >= INDEX_TRIGGER || log.getSize() > LOG_SIZE_TRIGGER) {
            // the condition are met to trigger a checkpoint
            try {
                checkpointExecute();
            } catch (IOException exception) {
                Logging.get().error(exception);
            }
        }
    }

    @Override
    public void close() throws IOException {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                // already closed ...
                throw new IllegalStateException();
            if ((s & STATE_FLAG_CLOSING_LOCK) == STATE_FLAG_CLOSING_LOCK)
                // flag is already used, someone is already closing this log ...
                throw new IllegalStateException();
            if (state.compareAndSet(s, s | STATE_FLAG_CLOSING_LOCK))
                break;
        }

        try {
            if (janitorScheduler != null)
                janitorScheduler.close();
            // execute a last checkpoint before closing
            cleanup(true);
            storage.close();
            log.close();
        } finally {
            state.set(STATE_CLOSED);
        }
    }
}
