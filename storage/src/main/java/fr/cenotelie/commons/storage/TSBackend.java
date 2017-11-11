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
 * no writing thread can overlap with other threads accessing the same backend
 *
 * @author Laurent Wouters
 */
public class TSBackend extends StorageBackend {
    /**
     * The backend storage system
     */
    private final StorageBackend backend;
    /**
     * The access manager to use
     */
    private final TSAccessManager accessManager;

    /**
     * Initializes this structure
     *
     * @param backend The backend storage system
     */
    public TSBackend(StorageBackend backend) {
        this.backend = backend;
        this.accessManager = new TSAccessManager(backend);
    }

    @Override
    public boolean isWritable() {
        return backend.isWritable();
    }

    @Override
    public long getSize() {
        return backend.getSize();
    }

    @Override
    public boolean truncate(long length) throws IOException {
        try (StorageAccess access = access(length, (int) backend.getSize(), true)) {

        }
        if (length >= backend.getSize())
            return false;

        backend.truncate(length);
    }

    @Override
    public void flush() throws IOException {
        backend.flush();
    }

    @Override
    public StorageEndpoint acquireEndpointAt(long index) {
        throw new UnsupportedOperationException("This operation is not allowed");
    }

    @Override
    public void releaseEndpoint(StorageEndpoint endpoint) {
        throw new UnsupportedOperationException("This operation is not allowed");
    }

    @Override
    public StorageAccess access(long location, int length, boolean writable) {
        if (location > Integer.MAX_VALUE)
            throw new IndexOutOfBoundsException("Location must be less than Integer.MAX_VALUE");
        return accessManager.get((int) location, length, backend.isWritable() && writable);
    }

    /**
     * Gets an access to the associated backend for the specified span
     *
     * @param location The location of the span within the backend
     * @param length   The length of the allowed span
     * @param writable Whether the access allows writing
     * @return The new access, or null if it cannot be obtained
     */
    public StorageAccess access(int location, int length, boolean writable) {
        return accessManager.get(location, length, backend.isWritable() && writable);
    }

    @Override
    public void close() throws IOException {
        backend.close();
    }
}
