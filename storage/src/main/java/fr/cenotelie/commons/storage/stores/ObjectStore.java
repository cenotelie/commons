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

import java.io.IOException;

/**
 * An object store enables the storage of serialized objects in a storage system.
 * An object store is backend by an underlying storage system.
 *
 * @author Laurent Wouters
 */
public abstract class ObjectStore implements AutoCloseable {
    /**
     * The null entry key, denotes the absence of value for a key
     */
    public static final long KEY_NULL = 0xFFFFFFFFFFFFFFFFL;

    /**
     * Allocates an object with the specified size
     *
     * @param size The size of the object to allocate
     * @return The location of the allocated object
     */
    public abstract long allocate(int size);

    /**
     * Frees the object at the specified location
     *
     * @param object The location of an object
     */
    public abstract void free(long object);

    /**
     * Access the object at the specified location
     *
     * @param object  The location of an object
     * @param writing Whether to allow writing
     * @return The access to the object
     */
    public abstract Access access(long object, boolean writing);

    /**
     * Flushes any outstanding changes to this storage system
     *
     * @throws IOException When an IO error occurred
     */
    public abstract void flush() throws IOException;

    /**
     * Closes this resource, relinquishing any underlying resources
     *
     * @throws IOException When an IO error occurred
     */
    @Override
    public abstract void close() throws IOException;
}
