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
import fr.cenotelie.commons.storage.Storage;

import java.io.IOException;

/**
 * Implements a simple object store.
 * The content of the store is thread-safe, if and only if the provided underlying storage system is thread-safe.
 * <p>
 * The first page has a special structure with the following layout
 * - int: Magic identifier for the store
 * - int: Layout version
 * - int: Start offset to free space
 * - int: Number of pools of reusable objects
 * - Array pool heads:
 * - int: Size of the objects in this pool
 * - long: Index of the first reusable object in the pool
 * <p>
 * An object stored in this file has the layout:
 * - header: char: object size
 * - content: the object
 *
 * @author Laurent Wouters
 */
public class ObjectStoreSimple extends ObjectStore {
    /**
     * Magic identifier of the type of store
     */
    private static final int MAGIC_ID = 0x784F574C;
    /**
     * The layout version
     */
    private static final int LAYOUT_VERSION = 1;
    /**
     * The size of the header in the preamble block
     * int: Magic identifier for the store
     * int: Layout version
     * int: Start offset to free space
     * int: Number of pools of reusable objects
     */
    private static final int PREAMBLE_HEADER_SIZE = 4 + 4 + 4 + 4;
    /**
     * The size of an open pool entry in the preamble
     * int: the size of objects in this pool
     * long: The location of the first re-usable object in this pool
     */
    private static final int PREAMBLE_ENTRY_SIZE = 4 + 8;
    /**
     * The maximum number of pools in this store
     */
    private static final int MAX_POOLS = (Constants.PAGE_SIZE - PREAMBLE_HEADER_SIZE) / PREAMBLE_ENTRY_SIZE;
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
     * The underlying storage system
     */
    private final Storage storage;

    /**
     * Initializes this store
     *
     * @param storage The underlying storage system
     */
    public ObjectStoreSimple(Storage storage) {
        this.storage = storage;
        if (storage.isWritable() && storage.getSize() < PREAMBLE_HEADER_SIZE) {
            try (Access access = storage.access(0, PREAMBLE_HEADER_SIZE, true)) {
                access.writeInt(MAGIC_ID);
                access.writeInt(LAYOUT_VERSION);
                access.writeInt(Constants.PAGE_SIZE);
                access.writeInt((char) 0);
            }
        }
    }

    @Override
    public long allocate(int size) {
        int toAllocate = size < OBJECT_MIN_SIZE ? OBJECT_MIN_SIZE : size;
        if (size > OBJECT_MAX_SIZE)
            throw new IndexOutOfBoundsException();
        try (Access access = storage.access(0, Constants.PAGE_SIZE, true)) {
            // get the number of pools
            int poolCount = access.seek(12).readInt();
            // look into the pools
            for (int i = 0; i != poolCount; i++) {
                int poolSize = access.readInt();
                long poolFirst = access.readLong();
                if (poolSize == toAllocate) {
                    // the pool size fits, try to reuse ...
                    if (poolFirst != 0)
                        // if the pool is not empty
                        return allocateReuse(access, i, poolFirst, poolSize);
                    // failed, stop looking into pools
                    break;
                }
            }
            // fall back to direct allocation from the free space
            return doAllocateDirect(access, toAllocate);
        }
    }


    /**
     * Tries to allocate an object by reusing the an empty entry in this store
     *
     * @param access    The access to the preambule
     * @param poolIndex The index of the pool to use
     * @param target    The first element in the pool
     * @param size      The size of the objects in the pool
     * @return The key to the object
     */
    private long allocateReuse(Access access, int poolIndex, long target, int size) {
        long next;
        try (Access accessTarget = storage.access(target, 8, true)) {
            next = accessTarget.readLong();
            accessTarget.reset().writeChar((char) size);
        }
        access.seek(PREAMBLE_HEADER_SIZE + poolIndex * PREAMBLE_ENTRY_SIZE + 4).writeLong(next);
        return target + 2;
    }

    /**
     * Tries to allocate an object of the specified size in this store
     * This method directly allocate the object without looking for reusable space.
     * Objects allocated with this method cannot be freed later.
     *
     * @param size The size of the object
     * @return The key to the object, or KEY_NULL if it cannot be allocated
     */
    public long allocateDirect(int size) {
        int toAllocate = size < OBJECT_MIN_SIZE ? OBJECT_MIN_SIZE : size;
        if (size > OBJECT_MAX_SIZE)
            throw new IndexOutOfBoundsException();
        try (Access access = storage.access(0, Constants.PAGE_SIZE, true)) {
            return doAllocateDirect(access, toAllocate);
        }
    }

    /**
     * Allocates at the end
     *
     * @param access The access to the preambule
     * @param size   The size of the object
     * @return The key to the object
     */
    private long doAllocateDirect(Access access, int size) {
        long freeSpace = access.seek(8).readInt();
        long target = freeSpace;
        freeSpace += size + OBJECT_HEADER_SIZE;
        if ((freeSpace & Constants.INDEX_MASK_UPPER) != (target & Constants.INDEX_MASK_UPPER)) {
            // not the same block, the object would be split between blocks
            // go to the next block entirely
            target = freeSpace & Constants.INDEX_MASK_UPPER;
            freeSpace = target + size + OBJECT_HEADER_SIZE;
        }
        access.seek(8).writeInt((int) freeSpace);
        try (Access accessTarget = storage.access(target, 2, true)) {
            accessTarget.writeChar((char) size);
        }
        return target + 2;
    }

    @Override
    public void free(long object) {
        // reads the length of the object
        int length;
        try (Access access = storage.access(object - 2, 2, false)) {
            length = access.readChar();
        }
        free(object, length);
    }

    /**
     * Frees the object at the specified location
     *
     * @param index  The location of an object in this store
     * @param length The expected length of the object to free
     */
    private void free(long index, int length) {
        // get the number of pools
        int poolCount;
        try (Access preamble = storage.access(0, PREAMBLE_HEADER_SIZE, true)) {
            poolCount = preamble.skip(12).readInt();

            if (poolCount > 0) {
                // at last one pool, try to find the corresponding size
                try (Access access2 = storage.access(PREAMBLE_HEADER_SIZE, poolCount * PREAMBLE_ENTRY_SIZE, true)) {
                    for (int i = 0; i != poolCount; i++) {
                        int poolSize = access2.readInt();
                        long poolHead = access2.readLong();
                        if (poolSize == length) {
                            // this is the pool we are looking for
                            // enqueue the pool head in place of the freed object
                            try (Access access3 = storage.access(index - 2, 8, true)) {
                                access3.writeLong(poolHead);
                            }
                            // replace the pool head
                            access2.seek(i * PREAMBLE_ENTRY_SIZE + 4).writeLong(index - 2);
                            // ok, finish here
                            return;
                        }
                    }
                }
            }

            // no corresponding pool, create one
            poolCount++;
            if (poolCount > MAX_POOLS)
                // cannot have more pool ...
                return;

            // enqueue an empty next pointer in place of the freed object
            try (Access access2 = storage.access(index - 2, 8, true)) {
                access2.writeLong(0);
            }
            // write the pool data
            try (Access access2 = storage.access(PREAMBLE_HEADER_SIZE, poolCount * PREAMBLE_ENTRY_SIZE, true)) {
                access2.writeInt(length);
                access2.writeLong(index - 2);
            }
            // increment the pool counter
            preamble.skip(12).writeInt(poolCount);
        }
    }

    @Override
    public Access access(long object, boolean writing) {
        int length;
        try (Access access = storage.access(object - 2, 2, false)) {
            length = access.readChar();
        }
        return storage.access(object, length, writing);
    }

    @Override
    public void flush() throws IOException {
        storage.flush();
    }

    @Override
    public void close() throws IOException {
        storage.close();
    }
}
