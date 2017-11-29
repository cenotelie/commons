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

import fr.cenotelie.commons.storage.*;
import fr.cenotelie.commons.utils.ByteUtils;

import java.io.IOException;

/**
 * Implements an object store that uses transactions to interact with the underlying storage system.
 * An object store provides an abstraction for the manipulation of serialized objects.
 *
 * @author Laurent Wouters
 */
public class ObjectStoreTransactional extends ObjectStore {
    /**
     * The underlying storage system
     */
    private final TransactionalStorage storage;

    /**
     * Initializes this store
     *
     * @param storage The underlying storage system
     * @throws ConcurrentWriteException when the initialization of the store failed
     */
    public ObjectStoreTransactional(TransactionalStorage storage) throws ConcurrentWriteException {
        this.storage = storage;
        if (storage.isWritable() && storage.getSize() < PREAMBLE_HEADER_SIZE) {
            try (Transaction transaction = storage.newTransaction(true, true)) {
                try (Access access = transaction.access(0, PREAMBLE_HEADER_SIZE, true)) {
                    access.writeLong(MAGIC_ID);
                    access.writeLong(Constants.PAGE_SIZE * 2);
                    access.writeInt(0);
                    access.writeInt(0);
                }
            }
        }
    }

    @Override
    public long allocate(int size) {
        Transaction transaction = storage.getTransaction();
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

    @Override
    public long allocateDirect(int size) {
        Transaction transaction = storage.getTransaction();
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
        long freeSpace = access.seek(8).readLong();
        long target = freeSpace;
        freeSpace += size + OBJECT_HEADER_SIZE;
        access.seek(8).writeLong(freeSpace);
        try (Access accessTarget = transaction.access(target, 2, true)) {
            accessTarget.writeChar((char) size);
        }
        return target + 2;
    }

    @Override
    public void free(long object) {
        Transaction transaction = storage.getTransaction();
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

    @Override
    public Access access(long object, boolean writing) {
        Transaction transaction = storage.getTransaction();
        int length;
        try (Access access = transaction.access(object - 2, 2, false)) {
            length = access.readChar();
        }
        return transaction.access(object, length, writing);
    }

    @Override
    public void register(String name, long object) {
        Transaction transaction = storage.getTransaction();
        int registered;
        try (Access preamble = transaction.access(0, PREAMBLE_HEADER_SIZE, true)) {
            registered = preamble.seek(8 + 8 + 4).readInt();
            if (registered >= MAX_REGISTERED)
                throw new IndexOutOfBoundsException();
            long id = ByteUtils.toLong(name);
            try (Access access = transaction.access(Constants.PAGE_SIZE, Constants.PAGE_SIZE, true)) {
                for (int i = 0; i != MAX_REGISTERED; i++) {
                    long itemLocation = access.skip(8).readLong();
                    if (itemLocation == 0) {
                        // reuse this entry
                        access.skip(-REGISTRY_ITEM_SIZE);
                        access.writeLong(id);
                        access.writeLong(object);
                        preamble.seek(8 + 8 + 4).writeInt(registered + 1);
                        return;
                    }
                }
            }
        }
    }

    @Override
    public long unregister(String name) {
        Transaction transaction = storage.getTransaction();
        int registered;
        try (Access preamble = transaction.access(0, PREAMBLE_HEADER_SIZE, true)) {
            registered = preamble.seek(8 + 8 + 4).readInt();
            if (registered == 0)
                return Constants.KEY_NULL;
            long id = ByteUtils.toLong(name);
            try (Access access = transaction.access(Constants.PAGE_SIZE, Constants.PAGE_SIZE, true)) {
                int found = 0;
                for (int i = 0; i != MAX_REGISTERED; i++) {
                    long itemId = access.readLong();
                    long itemLocation = access.readLong();
                    if (itemLocation == 0)
                        continue;
                    if (itemId != id) {
                        found++;
                        if (found == registered)
                            return Constants.KEY_NULL;
                        continue;
                    }
                    access.skip(-REGISTRY_ITEM_SIZE);
                    access.writeLong(0);
                    access.writeLong(0);
                    preamble.seek(8 + 8 + 4).writeInt(registered - 1);
                    return itemLocation;
                }
            }
        }
        return Constants.KEY_NULL;
    }

    @Override
    public long getObject(String name) {
        Transaction transaction = storage.getTransaction();
        int registered;
        try (Access preamble = transaction.access(0, PREAMBLE_HEADER_SIZE, false)) {
            registered = preamble.seek(8 + 8 + 4).readInt();
        }
        if (registered == 0)
            return Constants.KEY_NULL;
        long id = ByteUtils.toLong(name);
        try (Access access = transaction.access(Constants.PAGE_SIZE, Constants.PAGE_SIZE, false)) {
            int found = 0;
            for (int i = 0; i != MAX_REGISTERED; i++) {
                long itemId = access.readLong();
                long itemLocation = access.readLong();
                if (itemLocation == 0)
                    continue;
                if (itemId == id)
                    return itemLocation;
                found++;
                if (found == registered)
                    break;
            }
        }
        return Constants.KEY_NULL;
    }

    @Override
    public long getSize() {
        return storage.getSize();
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
