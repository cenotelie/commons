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
import fr.cenotelie.commons.storage.Transaction;
import fr.cenotelie.commons.storage.TransactionalStorage;

import java.io.IOException;

/**
 * Implements an object store that uses transactions to interact with the underlying storage system.
 * An object store provides an abstraction for the manipulation of serialized objects.
 *
 * @author Laurent Wouters
 */
public class TransactionalObjectStore extends ObjectStore {
    /**
     * The underlying storage system
     */
    private final TransactionalStorage storage;

    /**
     * Initializes this store
     *
     * @param storage The underlying storage system
     */
    public TransactionalObjectStore(TransactionalStorage storage) {
        this.storage = storage;
        if (storage.isWritable() && storage.getSize() < PREAMBLE_HEADER_SIZE) {
            try (Transaction transaction = storage.newTransaction(true, true)) {
                try (Access access = transaction.access(0, PREAMBLE_HEADER_SIZE, true)) {
                    access.writeLong(MAGIC_ID);
                    access.writeLong(Constants.PAGE_SIZE);
                    access.writeInt(0);
                }
            }
        }
    }

    /**
     * Allocates an object with the specified size
     * An attempt is made to reuse an previously freed object of the same size.
     *
     * @param transaction The transaction to use
     * @param size        The size of the object to allocate
     * @return The location of the allocated object
     */
    public long allocate(Transaction transaction, int size) {
        int toAllocate = size < OBJECT_MIN_SIZE ? OBJECT_MIN_SIZE : size;
        if (size > OBJECT_MAX_SIZE)
            throw new IndexOutOfBoundsException();
        try (Access access = transaction.access(0, Constants.PAGE_SIZE, true)) {
            // get the number of pools
            int poolCount = access.seek(8 + 8).readInt();
            // look into the pools
            for (int i = 0; i != poolCount; i++) {
                int poolSize = access.readInt();
                long poolFirst = access.readLong();
                if (poolSize == toAllocate) {
                    // the pool size fits, try to reuse ...
                    if (poolFirst != 0)
                        // if the pool is not empty
                        return doAllocateReusable(transaction, access, i, poolFirst, poolSize);
                    // failed, stop looking into pools
                    break;
                }
            }
            // fall back to direct allocation from the free space
            return doAllocateDirect(transaction, access, toAllocate);
        }
    }

    /**
     * Allocates an object with the specified size
     * This method directly allocate the object without looking for reusable space.
     * Objects allocated with this method cannot be freed later.
     *
     * @param transaction The transaction to use
     * @param size        The size of the object
     * @return The location of the allocated object
     */
    public long allocateDirect(Transaction transaction, int size) {
        int toAllocate = size < OBJECT_MIN_SIZE ? OBJECT_MIN_SIZE : size;
        if (size > OBJECT_MAX_SIZE)
            throw new IndexOutOfBoundsException();
        try (Access access = transaction.access(0, Constants.PAGE_SIZE, true)) {
            return doAllocateDirect(transaction, access, toAllocate);
        }
    }

    /**
     * Tries to allocate an object by reusing a previously freed object of the same size
     *
     * @param transaction The transaction to use
     * @param access      The access to the preambule
     * @param poolIndex   The index of the pool to use
     * @param target      The first element in the pool
     * @param size        The size of the objects in the pool
     * @return The key to the object
     */
    private long doAllocateReusable(Transaction transaction, Access access, int poolIndex, long target, int size) {
        long next;
        try (Access accessTarget = transaction.access(target, 8, true)) {
            next = accessTarget.readLong();
            accessTarget.reset().writeChar((char) size);
        }
        access.seek(PREAMBLE_HEADER_SIZE + poolIndex * PREAMBLE_ENTRY_SIZE + 4).writeLong(next);
        return target + 2;
    }

    /**
     * Directly allocate an object in the free space
     *
     * @param transaction The transaction to use
     * @param access      The access to the preambule
     * @param size        The size of the object
     * @return The key to the object
     */
    private long doAllocateDirect(Transaction transaction, Access access, int size) {
        long freeSpace = access.seek(8).readInt();
        long target = freeSpace;
        freeSpace += size + OBJECT_HEADER_SIZE;
        access.seek(8).writeLong(freeSpace);
        try (Access accessTarget = transaction.access(target, 2, true)) {
            accessTarget.writeChar((char) size);
        }
        return target + 2;
    }

    /**
     * Frees the object at the specified location
     *
     * @param transaction The transaction to use
     * @param object      The location of an object
     */
    public void free(Transaction transaction, long object) {
        // reads the length of the object
        int length;
        try (Access access = transaction.access(object - 2, 2, false)) {
            length = access.readChar();
        }

        // get the number of pools
        int poolCount;
        try (Access preamble = transaction.access(0, Constants.PAGE_SIZE, true)) {
            poolCount = preamble.seek(8 + 8).readInt();
            if (poolCount > 0) {
                // at last one pool, try to find the corresponding size
                for (int i = 0; i != poolCount; i++) {
                    int poolSize = preamble.readInt();
                    long poolHead = preamble.readLong();
                    if (poolSize == length) {
                        // this is the pool we are looking for
                        // enqueue the pool head in place of the freed object
                        try (Access access = transaction.access(object - 2, 8, true)) {
                            access.writeLong(poolHead);
                        }
                        // replace the pool head
                        preamble.seek(PREAMBLE_HEADER_SIZE + i * PREAMBLE_ENTRY_SIZE + 4).writeLong(object - 2);
                        // ok, finish here
                        return;
                    }
                }
            }

            // no corresponding pool, create one
            if (poolCount >= MAX_POOLS)
                // cannot have more pool ...
                return;

            // enqueue an empty next pointer in place of the freed object
            try (Access access = transaction.access(object - 2, 8, true)) {
                access.writeLong(0);
            }
            // write the pool data
            preamble.seek(PREAMBLE_HEADER_SIZE + poolCount * PREAMBLE_ENTRY_SIZE);
            preamble.writeInt(length);
            preamble.writeLong(object - 2);
            // increment the pool counter
            preamble.seek(8 + 8).writeInt(poolCount + 1);
        }
    }

    /**
     * Access the object at the specified location
     *
     * @param transaction The transaction to use
     * @param object      The location of an object
     * @param writing     Whether to allow writing
     * @return The access to the object
     */
    public Access access(Transaction transaction, long object, boolean writing) {
        int length;
        try (Access access = transaction.access(object - 2, 2, false)) {
            length = access.readChar();
        }
        return transaction.access(object, length, writing);
    }

    /**
     * Flushes any outstanding changes to this storage system
     *
     * @throws IOException When an IO error occurred
     */
    public void flush() throws IOException {
        storage.flush();
    }

    /**
     * Closes this resource, relinquishing any underlying resources
     *
     * @throws IOException When an IO error occurred
     */
    @Override
    public void close() throws IOException {
        storage.close();
    }
}
