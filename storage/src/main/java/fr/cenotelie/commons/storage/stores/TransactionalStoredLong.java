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

package fr.cenotelie.commons.storage.stores;

import fr.cenotelie.commons.storage.Access;
import fr.cenotelie.commons.storage.Transaction;

/**
 * Represents a long value stored in a transactional object store
 *
 * @author Laurent Wouters
 */
public class TransactionalStoredLong {
    /**
     * The backing store
     */
    private final TransactionalObjectStore store;
    /**
     * The entry in the store
     */
    private final long entry;

    /**
     * Initializes this persisted value from a stored one
     *
     * @param store The backing store
     * @param entry The entry in the store
     */
    public TransactionalStoredLong(TransactionalObjectStore store, long entry) {
        this.store = store;
        this.entry = entry;
    }

    /**
     * Creates a new persisted value
     *
     * @param store       The backing store
     * @param transaction The transaction to use
     * @param initValue   The initial value
     * @return The persisted value
     */
    public static TransactionalStoredLong create(TransactionalObjectStore store, Transaction transaction, long initValue) {
        long entry = store.allocate(transaction, 8);
        try (Access access = store.access(transaction, entry, true)) {
            access.writeLong(initValue);
        }
        return new TransactionalStoredLong(store, entry);
    }

    /**
     * Gets the value
     *
     * @param transaction The transaction to use
     * @return The value
     */
    public long get(Transaction transaction) {
        try (Access access = store.access(transaction, entry, false)) {
            return access.readLong();
        }
    }

    /**
     * Sets the value
     *
     * @param transaction The transaction to use
     * @param value       The value to set
     */
    public void set(Transaction transaction, long value) {
        try (Access access = store.access(transaction, entry, true)) {
            access.writeLong(value);
        }
    }

    /**
     * Compares and set the value
     *
     * @param transaction The transaction to use
     * @param expected    The expected old value
     * @param newValue    The new value to set
     * @return Whether the operation succeeded
     */
    public boolean compareAndSet(Transaction transaction, long expected, long newValue) {
        try (Access access = store.access(transaction, entry, true)) {
            long old = access.readLong();
            if (expected != old)
                return false;
            access.reset().writeLong(newValue);
            return true;
        }
    }

    /**
     * Gets the value and increment it
     *
     * @param transaction The transaction to use
     * @return The value before the increment
     */
    public long getAndIncrement(Transaction transaction) {
        try (Access access = store.access(transaction, entry, true)) {
            long value = access.readLong();
            access.reset().writeLong(value + 1);
            return value;
        }
    }
}
