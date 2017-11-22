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

import java.io.IOException;

/**
 * Implements a thread-safe proxy for a storage system that ensures that
 * no writing thread can overlap with other threads accessing the same storage system.
 *
 * @author Laurent Wouters
 */
public class ThreadSafeStorage extends Storage {
    /**
     * The storage system to proxy
     */
    private final Storage storage;
    /**
     * The access manager to use
     */
    private final ThreadSafeAccessManager accessManager;

    /**
     * Initializes this structure
     *
     * @param storage The storage system to proxy
     */
    public ThreadSafeStorage(Storage storage) {
        this.storage = storage;
        this.accessManager = new ThreadSafeAccessManager(storage);
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
    public boolean truncate(long length) throws IOException {
        if (length > Integer.MAX_VALUE)
            throw new IndexOutOfBoundsException("Length must be less than Integer.MAX_VALUE");
        long size = storage.getSize();
        if (size <= length)
            return false;
        try (Access access = access(length, (int) (size - length), true)) {
            return storage.truncate(length);
        }
    }

    @Override
    public void flush() throws IOException {
        storage.flush();
    }

    @Override
    public Endpoint acquireEndpointAt(long index) {
        throw new UnsupportedOperationException("This operation is not allowed");
    }

    @Override
    public void releaseEndpoint(Endpoint endpoint) {
        throw new UnsupportedOperationException("This operation is not allowed");
    }

    @Override
    public Access access(long location, int length, boolean writable) {
        if (location > Integer.MAX_VALUE)
            throw new IndexOutOfBoundsException("Location must be less than Integer.MAX_VALUE");
        return accessManager.get((int) location, length, storage.isWritable() && writable);
    }

    /**
     * Gets an access to the associated storage system for the specified span
     *
     * @param location The location of the span within the storage system
     * @param length   The length of the allowed span
     * @param writable Whether the access allows writing
     * @return The new access, or null if it cannot be obtained
     */
    public Access access(int location, int length, boolean writable) {
        return accessManager.get(location, length, storage.isWritable() && writable);
    }

    @Override
    public void close() throws IOException {
        storage.close();
    }
}
