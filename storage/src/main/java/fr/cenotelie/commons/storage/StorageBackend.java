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
 * Represents a backend storage system for IO operations.
 *
 * @author Laurent Wouters
 */
public abstract class StorageBackend implements AutoCloseable {
    /**
     * Gets whether this storage system can be written to
     *
     * @return Whether this storage system can be written to
     */
    public abstract boolean isWritable();

    /**
     * Gets the current size of this storage system
     *
     * @return The current size of this storage system
     */
    public abstract long getSize();

    /**
     * Truncates this storage system to the specified length
     *
     * @param length The length to truncate to
     * @return Whether the operation had an effect
     * @throws IOException When an IO error occurred
     */
    public abstract boolean truncate(long length) throws IOException;

    /**
     * Flushes any outstanding changes to this storage system
     *
     * @throws IOException When an IO error occurred
     */
    public abstract void flush() throws IOException;

    /**
     * Acquires an endpoint that enables reading and writing to the backend at the specified index
     * The endpoint must be subsequently released by a call to
     *
     * @param index An index within this backend
     * @return The corresponding endpoint
     */
    public abstract StorageEndpoint acquireEndpointAt(long index);

    /**
     * When an endpoint is no longer required
     *
     * @param endpoint The endpoint to release
     */
    public abstract void releaseEndpoint(StorageEndpoint endpoint);

    /**
     * Gets an access to the associated backend for the specified span
     *
     * @param location The location of the span within the backend
     * @param length   The length of the allowed span
     * @param writable Whether the access allows writing
     * @return The new access, or null if it cannot be obtained
     */
    public StorageAccess access(long location, int length, boolean writable) {
        return new StorageAccess(this, location, length, isWritable() && writable);
    }

    /**
     * Closes this resource, relinquishing any underlying resources
     *
     * @throws IOException When an IO error occurred
     */
    @Override
    public abstract void close() throws IOException;
}
