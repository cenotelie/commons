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

/**
 * Represents a long value that is persisted in a storage system.
 *
 * @author Laurent Wouters
 */
public class StoredLong {
    /**
     * The backing store
     */
    private final ObjectStore store;
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
    public StoredLong(ObjectStore store, long entry) {
        this.store = store;
        this.entry = entry;
    }

    /**
     * Creates a new persisted value
     *
     * @param store     The backing store
     * @param initValue The initial value
     * @return The persisted value
     */
    public static StoredLong create(ObjectStore store, long initValue) {
        long entry = store.allocate(8);
        try (Access access = store.access(entry, true)) {
            access.writeLong(initValue);
        }
        return new StoredLong(store, entry);
    }

    /**
     * Gets the value
     *
     * @return The value
     */
    public long get() {
        try (Access access = store.access(entry, false)) {
            return access.readLong();
        }
    }

    /**
     * Sets the value
     *
     * @param value The value to set
     */
    public void set(long value) {
        try (Access access = store.access(entry, true)) {
            access.writeLong(value);
        }
    }

    /**
     * Compares and set the value
     *
     * @param expected The expected old value
     * @param newValue The new value to set
     * @return Whether the operation succeeded
     */
    public boolean compareAndSet(long expected, long newValue) {
        try (Access access = store.access(entry, true)) {
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
     * @return The value before the increment
     */
    public long getAndIncrement() {
        try (Access access = store.access(entry, true)) {
            long value = access.readLong();
            access.reset().writeLong(value + 1);
            return value;
        }
    }
}
