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

import java.util.Objects;

/**
 * Represents an object stored in a transactional object store
 *
 * @param <T> The type of the object
 * @author Laurent Wouters
 */
public class TransactionalStoredObject<T> {
    /**
     * The backing store
     */
    private final TransactionalObjectStore store;
    /**
     * The entry in the store
     */
    private final long entry;
    /**
     * The object mediator to use
     */
    private final ObjectMediator<T> mediator;

    /**
     * Initializes this persisted value from a stored one
     *
     * @param store    The backing store
     * @param entry    The entry in the store
     * @param mediator The object mediator to use
     */
    public TransactionalStoredObject(TransactionalObjectStore store, long entry, ObjectMediator<T> mediator) {
        this.store = store;
        this.entry = entry;
        this.mediator = mediator;
    }

    /**
     * Creates a stored object
     *
     * @param store       The backing store
     * @param transaction The transaction to use
     * @param mediator    The mediator to use
     * @param initValue   The initial value
     * @param <T>         The type of the object
     * @return The stored object
     */
    public static <T> TransactionalStoredObject<T> create(TransactionalObjectStore store, Transaction transaction, ObjectMediator<T> mediator, T initValue) {
        long entry = store.allocate(transaction, mediator.getSerializationSize());
        try (Access access = store.access(transaction, entry, true)) {
            mediator.write(access, initValue);
        }
        return new TransactionalStoredObject<>(store, entry, mediator);
    }

    /**
     * Gets the value
     *
     * @param transaction The transaction to use
     * @return The value
     */
    public T get(Transaction transaction) {
        try (Access access = store.access(transaction, entry, false)) {
            return mediator.read(access);
        }
    }

    /**
     * Sets the value
     *
     * @param transaction The transaction to use
     * @param value       The value to set
     */
    public void set(Transaction transaction, T value) {
        try (Access access = store.access(transaction, entry, true)) {
            mediator.write(access, value);
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
    public boolean compareAndSet(Transaction transaction, T expected, T newValue) {
        try (Access access = store.access(transaction, entry, true)) {
            T old = mediator.read(access);
            if (!Objects.equals(expected, old))
                return false;
            access.reset();
            mediator.write(access, newValue);
            return true;
        }
    }
}
