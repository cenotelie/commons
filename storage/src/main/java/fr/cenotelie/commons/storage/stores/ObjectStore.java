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
import fr.cenotelie.commons.storage.Constants;

import java.io.IOException;

/**
 * An object store enables the storage of serialized objects in a storage system.
 * An object store is backend by an underlying storage system.
 *
 * @author Laurent Wouters
 */
public abstract class ObjectStore implements AutoCloseable {
    /**
     * Magic identifier of the type of store
     */
    protected static final long MAGIC_ID = 0x63656e2d6f626a73L; // cen-objs
    /**
     * The size of the header in the preamble block
     * long: Magic identifier for the store
     * long: Start offset to free space
     * int: Number of pools of reusable object pools
     */
    protected static final int PREAMBLE_HEADER_SIZE = 8 + 8 + 4;
    /**
     * The size of an open pool entry in the preamble
     * int: the size of objects in this pool
     * long: The location of the first re-usable object in this pool
     */
    protected static final int PREAMBLE_ENTRY_SIZE = 4 + 8;
    /**
     * The maximum number of pools in this store
     */
    protected static final int MAX_POOLS = (Constants.PAGE_SIZE - PREAMBLE_HEADER_SIZE) / PREAMBLE_ENTRY_SIZE;
    /**
     * Size of the header for each stored object
     */
    public static final int OBJECT_HEADER_SIZE = 2;
    /**
     * Minimum size of objects in this store
     */
    public static final int OBJECT_MIN_SIZE = 8 - OBJECT_HEADER_SIZE;
    /**
     * Maximum size of objects in this store
     */
    public static final int OBJECT_MAX_SIZE = Constants.PAGE_SIZE - OBJECT_HEADER_SIZE;

    /**
     * Allocates an object with the specified size
     * An attempt is made to reuse an previously freed object of the same size.
     *
     * @param size The size of the object to allocate
     * @return The location of the allocated object
     */
    public abstract long allocate(int size);

    /**
     * Tries to allocate an object of the specified size in this store
     * This method directly allocate the object without looking for reusable space.
     * Objects allocated with this method cannot be freed later.
     *
     * @param size The size of the object
     * @return The key to the object, or KEY_NULL if it cannot be allocated
     */
    public abstract long allocateDirect(int size);

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
     * Gets the current size of this store
     *
     * @return The current size of this store
     */
    public abstract long getSize();

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
