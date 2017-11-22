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
 * Represents a storage system that can be read from and written to.
 * A storage system basically provides an abstraction for a virtually infinite array of bytes.
 * However, a system storage can only be read from and written to in a controlled manner through Accesses.
 * Accesses are obtained by the <code>access</code> method in this class.
 * <p>
 * A storage system must provide endpoints, which are an abstraction over different part of the storage system.
 * Depending on the index (location within the storage) that is requested, different endpoints may be used.
 * Endpoints are generally not manipulated by API users, as they are transparently requested and released by Accesses.
 *
 * @author Laurent Wouters
 */
public abstract class Storage implements AutoCloseable {
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
    public boolean truncate(long length) throws IOException {
        return cut(length, Long.MAX_VALUE);
    }

    /**
     * Cuts content within this storage system
     *
     * @param from The starting index to cut at (included)
     * @param to   The end index to cut to (excluded)
     * @return Whether the operation had an effect
     * @throws IOException When an IO error occurred
     */
    public abstract boolean cut(long from, long to) throws IOException;

    /**
     * Flushes any outstanding changes to this storage system
     *
     * @throws IOException When an IO error occurred
     */
    public abstract void flush() throws IOException;

    /**
     * Acquires an endpoint that enables reading and writing to the storage system at the specified index
     * The endpoint must be subsequently released by a call to
     *
     * @param index An index within this storage system
     * @return The corresponding endpoint
     */
    public abstract Endpoint acquireEndpointAt(long index);

    /**
     * When an endpoint is no longer required
     *
     * @param endpoint The endpoint to release
     */
    public abstract void releaseEndpoint(Endpoint endpoint);

    /**
     * Gets an access to the associated storage system for the specified span
     *
     * @param location The location of the span within the storage system
     * @param length   The length of the allowed span
     * @param writable Whether the access allows writing
     * @return The new access, or null if it cannot be obtained
     */
    public Access access(long location, int length, boolean writable) {
        return new Access(this, location, length, isWritable() && writable);
    }

    /**
     * Closes this resource, relinquishing any underlying resources
     *
     * @throws IOException When an IO error occurred
     */
    @Override
    public abstract void close() throws IOException;
}
