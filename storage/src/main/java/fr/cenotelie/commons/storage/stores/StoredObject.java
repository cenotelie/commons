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

import java.util.Objects;

/**
 * Represents an object that is persisted in a storage system.
 *
 * @param <T> The type of the object
 * @author Laurent Wouters
 */
public class StoredObject<T> {
    /**
     * The backing store
     */
    private final ObjectStore store;
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
    public StoredObject(ObjectStore store, long entry, ObjectMediator<T> mediator) {
        this.store = store;
        this.entry = entry;
        this.mediator = mediator;
    }

    /**
     * Creates a stored object
     *
     * @param store     The backing store
     * @param mediator  The mediator to use
     * @param initValue The initial value
     * @param <T>       The type of the object
     * @return The stored object
     */
    public static <T> StoredObject<T> create(ObjectStore store, ObjectMediator<T> mediator, T initValue) {
        long entry = store.allocate(mediator.getSerializationSize());
        try (Access access = store.access(entry, true)) {
            mediator.write(access, initValue);
        }
        return new StoredObject<>(store, entry, mediator);
    }

    /**
     * Gets the value
     *
     * @return The value
     */
    public T get() {
        try (Access access = store.access(entry, false)) {
            return mediator.read(access);
        }
    }

    /**
     * Sets the value
     *
     * @param value The value to set
     */
    public void set(T value) {
        try (Access access = store.access(entry, true)) {
            mediator.write(access, value);
        }
    }

    /**
     * Compares and set the value
     *
     * @param expected The expected old value
     * @param newValue The new value to set
     * @return Whether the operation succeeded
     */
    public boolean compareAndSet(T expected, T newValue) {
        try (Access access = store.access(entry, true)) {
            T old = mediator.read(access);
            if (!Objects.equals(expected, old))
                return false;
            access.reset();
            mediator.write(access, newValue);
            return true;
        }
    }
}
