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

package fr.cenotelie.commons.storage;

import java.util.ConcurrentModificationException;

/**
 * Represents a user transaction for a write-ahead log that can be used to perform reading and writing
 * A transaction is expected to be used by one thread only, and is only usable and the thread that created the transaction.
 * A transaction MUST be closed.
 *
 * @author Laurent Wouters
 */
public abstract class Transaction implements AutoCloseable {
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
     * The thread that created and is running this transaction
     */
    protected final Thread thread;
    /**
     * Whether this transaction allows writing
     */
    protected final boolean writable;
    /**
     * Whether this transaction should commit when being closed
     */
    protected final boolean autocommit;
    /**
     * The current state of this transaction
     */
    private int state;

    /**
     * Initializes this transaction
     *
     * @param writable   Whether this transaction allows writing
     * @param autocommit Whether this transaction should commit when being closed
     */
    public Transaction(boolean writable, boolean autocommit) {
        this.thread = Thread.currentThread();
        this.writable = writable;
        this.autocommit = autocommit;
        this.state = STATE_RUNNING;
    }

    /**
     * Gets the thread that runs this transaction
     *
     * @return The thread that created and is running this transaction
     */
    public Thread getThread() {
        return thread;
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
     * @throws ConcurrentModificationException when a concurrent transaction already committed conflicting changes to the log
     */
    public void commit() throws ConcurrentModificationException {
        if (thread != Thread.currentThread() && thread.isAlive())
            throw new WrongThreadException();
        if (state != STATE_RUNNING)
            throw new IllegalStateException();
        state = STATE_COMMITTING;
        try {
            doCommit();
            state = STATE_COMMITTED;
        } catch (ConcurrentModificationException exception) {
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
     * Commits this transaction
     *
     * @throws ConcurrentModificationException when a concurrent transaction already committed conflicting changes
     */
    protected abstract void doCommit() throws ConcurrentModificationException;

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
     * @throws ConcurrentModificationException when a concurrent transaction already committed conflicting changes to the log
     */
    @Override
    public void close() throws ConcurrentModificationException {
        if (thread != Thread.currentThread() && thread.isAlive())
            throw new WrongThreadException();
        if (state == STATE_RUNNING) {
            if (autocommit)
                commit();
            else
                state = STATE_ABORTED; // abort this transaction
        }
        onClose();
    }

    /**
     * When this transaction is being closed
     */
    protected void onClose() {
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
        return newAccess(index, length, writable);
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
    protected abstract Access newAccess(long index, int length, boolean writable);
}