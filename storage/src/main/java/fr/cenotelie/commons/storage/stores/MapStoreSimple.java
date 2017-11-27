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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Implements a (long -> long) map that is persisted in a storage system.
 * A persisted map is thread-safe for access and modifications.
 * <p>
 * A persisted map is implemented as a B+ tree with Preparatory Operations.
 * Y. Mond and Y. Raz, Concurrency Control in B+ Tree Databases using Preparatory Operations, in Proceedings of VLDB 1985
 * <p>
 * n is the rate of the tree
 * Invariant: The number of keys (k) of a node is in [n-1, 2n+1].
 * => The number of keys of a node is in [n, 2n+2].
 * Invariant: Exception to the above is the root node.
 * Invariant: A node that is not a leaf has at most k keys and k+1 children.
 * Invariant: The keyed data are all at the leaves.
 * <p>
 * The leaf nodes contains a pointer to their right neighbour, i.e. the next leaf node.
 * This pointer can be used for iterating over the leaf nodes.
 *
 * @author Laurent Wouters
 */
class MapStoreSimple {
    /**
     * The rate of the B+ tree
     */
    private static final int N = 15;
    /**
     * The maximum number of child references in a node (2*n + 2)
     */
    private static final int CHILD_COUNT = N * 2 + 2;
    /**
     * The size of a child entry in a B+ tree node:
     * int: key value
     * long: pointer to child
     */
    private static final int CHILD_SIZE = 8 + 8;
    /**
     * The size of a B+ tree inner node header
     * long: pointer to the parent node
     * char: leaf marker
     * char: number of keys
     */
    private static final int NODE_HEADER = 8 + 2 + 2;
    /**
     * The size of a B+ tree node in stage 2:
     * header: the node header
     * entries[entryCount]: the children entries
     */
    private static final int NODE_SIZE = NODE_HEADER + CHILD_COUNT * CHILD_SIZE;
    /**
     * Marker for leaf nodes
     */
    private static final char NODE_IS_LEAF = 1;

    /**
     * Represents an entry in this map
     */
    public static class Entry {
        /**
         * The key for this entry
         */
        public long key;
        /**
         * The value associated to the key
         */
        public long value;

        /**
         * Initializes this entry
         *
         * @param key   The key for this entry
         * @param value The value associated to the key
         */
        public Entry(long key, long value) {
            this.key = key;
            this.value = value;
        }

        /**
         * Resets this entry
         *
         * @param key   The key for this entry
         * @param value The value associated to the key
         */
        private void reset(long key, long value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * The backing store
     */
    private final ObjectStoreSimple store;
    /**
     * The head entry for the map
     */
    private final long head;

    /**
     * Initializes this map
     *
     * @param store The backing store
     * @param head  The head entry for the map
     */
    public MapStoreSimple(ObjectStoreSimple store, long head) {
        this.store = store;
        this.head = head;
    }

    /**
     * Creates a new persisted map
     *
     * @param store The backing store
     * @return The persisted map
     */
    public static MapStoreSimple create(ObjectStoreSimple store) {
        long entry = store.allocate(NODE_SIZE);
        try (Access access = store.access(entry, true)) {
            // write header
            access.writeLong(Constants.KEY_NULL);
            access.writeChar(NODE_IS_LEAF);
            access.writeChar((char) 0);
            // write last entry to non-existing node
            access.writeLong(0);
            access.writeLong(Constants.KEY_NULL);
        }
        return new MapStoreSimple(store, entry);
    }

    /**
     * Gets the value associated to key
     *
     * @param key The requested key
     * @return The associated value, or ObjectStore.KEY_NULL when the key is not present
     */
    public long get(long key) {
        Access accessFather = null;
        Access accessCurrent = store.access(head, false);
        try {
            while (true) {
                // inspect the current node
                boolean isLeaf = accessCurrent.skip(8).readChar() == NODE_IS_LEAF;
                char count = accessCurrent.readChar();
                if (isLeaf)
                    return getOnNode(accessCurrent, key, count);
                long next = getChild(accessCurrent, key, count);
                // free the father if any, rotate the accesses and access the next node
                if (accessFather != null)
                    accessFather.close();
                accessFather = accessCurrent;
                accessCurrent = store.access(next, false);
            }
        } finally {
            if (accessFather != null)
                accessFather.close();
            if (accessCurrent != null)
                accessCurrent.close();
        }
    }

    /**
     * When on a leaf node, look for the matching entry
     *
     * @param accessCurrent The access to the current node
     * @param key           The stage 2 key to look for
     * @param count         The number of entries in the node
     * @return The associated value, or ObjectStore.KEY_NULL when none is found
     */
    private long getOnNode(Access accessCurrent, long key, char count) {
        for (int i = 0; i != count; i++) {
            // read entry data
            long entryKey = accessCurrent.readLong();
            long entryPtr = accessCurrent.readLong();
            if (entryKey == key) {
                // found the key
                return entryPtr;
            }
        }
        return Constants.KEY_NULL;
    }

    /**
     * When on an internal node, look for a suitable descendant
     *
     * @param accessCurrent The access to the current node
     * @param key           The stage 2 key to look for
     * @param count         The number of entries in the node
     * @return The descendant
     */
    private long getChild(Access accessCurrent, long key, char count) {
        for (int i = 0; i != count; i++) {
            // read entry data
            long entryKey = accessCurrent.readLong();
            long entryPtr = accessCurrent.readLong();
            if (key < entryKey) {
                // go through this child
                return entryPtr;
            }
        }
        return accessCurrent.skip(8).readLong();
    }

    /**
     * Atomically tries to insert a value in the map for a key
     * This fails if there already is an entry.
     *
     * @param key      The key
     * @param valueNew The associated value
     * @return Whether the operation succeeded
     */
    public boolean tryPut(long key, long valueNew) {
        return compareAndSet(key, Constants.KEY_NULL, valueNew);
    }

    /**
     * Atomically tries to remove a value from the map
     * This fails if there is no value or if the value to remove is different from the expected one.
     *
     * @param key      The key
     * @param valueOld The expected value to remove
     * @return Whether the operation succeeded
     */
    public boolean tryRemove(long key, long valueOld) {
        return compareAndSet(key, valueOld, Constants.KEY_NULL);
    }

    /**
     * Atomically replaces a value in the map for a key
     * This fails if there is no value for the key, or if the actual value is different from the expected one.
     *
     * @param key      The key
     * @param valueOld The old value to replace (ObjectStore.KEY_NULL, if this is expected to be an insertion)
     * @param valueNew The new value for the key (ObjectStore.KEY_NULL, if this is expected to be a removal)
     * @return Whether the operation succeeded
     */
    public boolean compareAndSet(long key, long valueOld, long valueNew) {
        Access accessFather = null;
        Access accessCurrent = store.access(head, true);
        try {
            inspect(null, accessCurrent, head, valueNew != Constants.KEY_NULL);
            while (true) {
                // inspect the current node
                boolean isLeaf = accessCurrent.seek(8).readChar() == NODE_IS_LEAF;
                char count = accessCurrent.readChar();
                if (isLeaf) {
                    // look into the entries of this node
                    for (int i = 0; i != count; i++) {
                        long entryKey = accessCurrent.readLong();
                        long entryValue = accessCurrent.readLong();
                        if (entryKey == key)
                            // found the key
                            return doCompareAndReplace(accessCurrent, i, count, entryValue, valueOld, valueNew);
                    }
                    // did not find the key, stop here
                    return doInsert(accessCurrent, count, key, valueOld, valueNew);
                }
                // release the father
                if (accessFather != null)
                    accessFather.close();
                // resolve the child
                long child = getChild(accessCurrent, key, count);
                Access accessChild = store.access(child, true);
                boolean hasChanged = inspect(accessCurrent, accessChild, child, valueNew != Constants.KEY_NULL);
                while (hasChanged) {
                    accessChild.close();
                    count = accessCurrent.seek(8 + 2).readChar();
                    child = getChild(accessCurrent, key, count);
                    accessChild = store.access(child, true);
                    hasChanged = inspect(accessCurrent, accessChild, child, valueNew != Constants.KEY_NULL);
                }
                // rotate
                accessFather = accessCurrent;
                accessCurrent = accessChild;
            }
        } finally {
            if (accessFather != null)
                accessFather.close();
            if (accessCurrent != null)
                accessCurrent.close();
        }
    }

    /**
     * When an existing key-value map is found in the current node, perform a compare and replace
     *
     * @param accessCurrent The access to the current node
     * @param index         The index of the entry within the node
     * @param count         The number of entries in the node
     * @param entryValue    The current value of the entry
     * @param valueOld      The old value to replace (ObjectStore.KEY_NULL, if this is expected to be an insertion)
     * @param valueNew      The new value for the key (ObjectStore.KEY_NULL, if this is expected to be a removal)
     * @return Whether the operation succeeded
     */
    private boolean doCompareAndReplace(Access accessCurrent, int index, char count, long entryValue, long valueOld, long valueNew) {
        if (entryValue != valueOld)
            // oops, compare failed
            return false;
        if (valueNew != Constants.KEY_NULL) {
            // this is a replace
            accessCurrent.skip(-8).writeLong(valueNew);
            return true;
        } else {
            // this is a removal
            // shift the data to the left
            for (int i = index + 1; i != count + 1; i++) {
                accessCurrent.seek(NODE_HEADER + i * CHILD_SIZE);
                long key = accessCurrent.readLong();
                long value = accessCurrent.readLong();
                accessCurrent.skip(-CHILD_SIZE * 2);
                accessCurrent.writeLong(key);
                accessCurrent.writeLong(value);
            }
            // update the header
            count--;
            accessCurrent.seek(8 + 2).writeChar(count);
            return true;
        }
    }

    /**
     * When the requested key is not found in the current leaf node, try to perform an insert
     *
     * @param accessCurrent The access to the current node
     * @param count         The number of entries in the node
     * @param key           The key
     * @param valueOld      The old value to replace (ObjectStore.KEY_NULL, if this is expected to be an insertion)
     * @param valueNew      The new value for the key (ObjectStore.KEY_NULL, if this is expected to be a removal)
     * @return Whether the operation succeeded
     */
    private boolean doInsert(Access accessCurrent, char count, long key, long valueOld, long valueNew) {
        if (valueNew != Constants.KEY_NULL) {
            // this is an insertion
            if (valueOld != Constants.KEY_NULL)
                // expected a value
                return false;
            // find the index to insert at
            int insertAt = count;
            accessCurrent.seek(NODE_HEADER);
            for (int i = 0; i != count; i++) {
                long entryKey = accessCurrent.readLong();
                accessCurrent.skip(8);
                if (entryKey > key) {
                    insertAt = i;
                    break;
                }
            }
            // shift the existing data to the right
            for (int i = count; i != insertAt - 1; i--) {
                accessCurrent.seek(NODE_HEADER + i * CHILD_SIZE);
                long entryKey = accessCurrent.readLong();
                long entryValue = accessCurrent.readLong();
                accessCurrent.writeLong(entryKey);
                accessCurrent.writeLong(entryValue);
            }
            // write the inserted key-value
            accessCurrent.seek(NODE_HEADER + insertAt * CHILD_SIZE);
            accessCurrent.writeLong(key);
            accessCurrent.writeLong(valueNew);
            // update the header
            count++;
            accessCurrent.seek(8 + 2).writeChar(count);
            return true;
        } else {
            // the entry is not found and the new value is the null key
            // this is only valid if the expected old value is also null
            return (valueOld == Constants.KEY_NULL);
        }
    }

    /**
     * Inspect the current node for preparatory operation
     *
     * @param accessFather  The access to the parent node
     * @param accessCurrent The access to the current node
     * @param current       The entry for the node to split
     * @param isInsert      Whether this is an insert operation
     * @return Whether the tree was modified
     */
    private boolean inspect(Access accessFather, Access accessCurrent, long current, boolean isInsert) {
        boolean isLeaf = accessCurrent.seek(8).readChar() == NODE_IS_LEAF;
        char count = accessCurrent.readChar();
        if (isInsert && count >= 2 * N) {
            // split the current node
            if (accessFather == null) {
                if (isLeaf)
                    splitRootLeaf(accessCurrent, current, count);
                else
                    splitRootInternal(accessCurrent, current, count);
            } else if (isLeaf)
                splitLeaf(accessFather, accessCurrent, current, count);
            else
                splitInternal(accessFather, accessCurrent, current, count);
            return true;
        } else if (!isInsert && accessFather != null && count <= N) {
            // the current node is a candidate for merging
            // find the left and right neighbours for the current node
            int fatherCount = accessFather.seek(8 + 2).readChar();
            long neighbourLeft = Constants.KEY_NULL;
            long neighbourRight = Constants.KEY_NULL;
            long previousValue = Constants.KEY_NULL;
            int indexCurrent = -1;
            for (int i = 0; i != fatherCount + 1; i++) {
                long value = accessFather.skip(8).readLong();
                if (value == current) {
                    neighbourLeft = previousValue;
                    if (i != fatherCount)
                        neighbourRight = accessFather.skip(8).readLong();
                    indexCurrent = i;
                    break;
                }
                previousValue = value;
            }

            // try to merge with the neighbour on the right if any
            if (neighbourRight != Constants.KEY_NULL) {
                boolean freeRight;
                try (Access accessRight = store.access(neighbourRight, true)) {
                    freeRight = isLeaf
                            ? tryMergeLeaves(accessFather, accessCurrent, accessRight, indexCurrent, neighbourRight)
                            : tryMergeInternals(accessFather, accessCurrent, accessRight, indexCurrent);
                }
                if (freeRight)
                    store.free(neighbourRight);
                return true;
            }

            // try to merge with the neighbour on the left if any
            if (neighbourLeft != Constants.KEY_NULL) {
                boolean freeRight;
                try (Access accessLeft = store.access(neighbourLeft, true)) {
                    freeRight = isLeaf
                            ? tryMergeLeaves(accessFather, accessLeft, accessCurrent, indexCurrent - 1, current)
                            : tryMergeInternals(accessFather, accessLeft, accessCurrent, indexCurrent - 1);
                }
                if (freeRight)
                    store.free(current);
                return true;
            }
        }
        return false;
    }

    /**
     * Splits the root node when it is an internal node
     *
     * @param accessCurrent The access to the current node
     * @param current       The entry for the node to split
     * @param count         The number of keys in the current node
     */
    private void splitRootInternal(Access accessCurrent, long current, char count) {
        // number of keys to transfer to the right (not including the fallback pointer)
        int countRight = (count == 2 * N) ? N - 1 : N;
        long left = store.allocate(NODE_SIZE);
        long right = store.allocate(NODE_SIZE);
        long maxLeft;
        // write the new left node
        accessCurrent.seek(NODE_HEADER);
        try (Access accessLeft = store.access(left, true)) {
            accessLeft.writeLong(current);
            accessLeft.writeChar((char) 0);
            accessLeft.writeChar((char) N);
            for (int i = 0; i != N; i++) {
                accessLeft.writeLong(accessCurrent.readLong());
                accessLeft.writeLong(accessCurrent.readLong());
            }
            maxLeft = accessCurrent.readLong();
            accessLeft.writeLong(0);
            accessLeft.writeLong(accessCurrent.readLong());
        }
        // write the new right node
        try (Access accessRight = store.access(right, true)) {
            accessRight.writeLong(current);
            accessRight.writeChar((char) 0);
            accessRight.writeChar((char) countRight);
            for (int i = 0; i != countRight + 1; i++) {
                accessRight.writeLong(accessCurrent.readLong());
                accessRight.writeLong(accessCurrent.readLong());
            }
        }
        // rewrite the root node
        accessCurrent.seek(8 + 2);
        accessCurrent.writeChar((char) 1); // only one key
        accessCurrent.writeLong(maxLeft);
        accessCurrent.writeLong(left);
        accessCurrent.writeLong(0);
        accessCurrent.writeLong(right);
    }

    /**
     * Splits the root node when it is a leaf
     *
     * @param accessCurrent The access to the current node
     * @param current       The entry for the node to split
     * @param count         The number of keys in the current node
     */
    private void splitRootLeaf(Access accessCurrent, long current, char count) {
        // number of keys to transfer to the right (not including the neighbour pointer)
        int countRight = (count == 2 * N) ? N : N + 1;
        long left = store.allocate(NODE_SIZE);
        long right = store.allocate(NODE_SIZE);
        // write the new left node
        accessCurrent.seek(NODE_HEADER);
        try (Access accessLeft = store.access(left, true)) {
            accessLeft.writeLong(current);
            accessLeft.writeChar(NODE_IS_LEAF);
            accessLeft.writeChar((char) N);
            for (int i = 0; i != N; i++) {
                accessLeft.writeLong(accessCurrent.readLong());
                accessLeft.writeLong(accessCurrent.readLong());
            }
            // write the leaf neighbour
            accessLeft.writeLong(0);
            accessLeft.writeLong(right);
        }
        // write the new right node
        long rightFirstKey = 0;
        try (Access accessRight = store.access(right, true)) {
            accessRight.writeLong(current);
            accessRight.writeChar(NODE_IS_LEAF);
            accessRight.writeChar((char) countRight);
            for (int i = 0; i != countRight; i++) {
                long key = accessCurrent.readLong();
                if (i == 0)
                    rightFirstKey = key;
                accessRight.writeLong(key);
                accessRight.writeLong(accessCurrent.readLong());
            }
            // write the leaf neighbour
            accessRight.writeLong(0);
            accessRight.writeLong(Constants.KEY_NULL);
        }
        // rewrite the root node
        accessCurrent.seek(8);
        accessCurrent.writeChar((char) 0);
        accessCurrent.writeChar((char) 1); // only one key
        accessCurrent.writeLong(rightFirstKey);
        accessCurrent.writeLong(left);
        accessCurrent.writeLong(0);
        accessCurrent.writeLong(right);
    }

    /**
     * Splits an internal node (that is not the root)
     *
     * @param accessFather  The access to the parent node
     * @param accessCurrent The access to the current node
     * @param current       The entry for the node to split
     * @param count         The number of keys in the current node
     */
    private void splitInternal(Access accessFather, Access accessCurrent, long current, char count) {
        // number of keys to transfer to the right (not including the fallback child pointer)
        int countRight = (count == 2 * N) ? N - 1 : N;
        accessCurrent.seek(NODE_HEADER + (N + 1) * CHILD_SIZE);
        long right = store.allocate(NODE_SIZE);
        try (Access accessRight = store.access(right, true)) {
            accessRight.writeLong(current);
            accessRight.writeChar((char) 0);
            accessRight.writeChar((char) countRight);
            // write the transferred key-values and the neighbour pointer
            for (int i = 0; i != countRight + 1; i++) {
                accessRight.writeLong(accessCurrent.readLong());
                accessRight.writeLong(accessCurrent.readLong());
            }
        }
        // update the data for the split node
        accessCurrent.seek(8 + 2).writeChar((char) N);
        long maxLeft = accessCurrent.seek(NODE_HEADER + N * CHILD_SIZE).readLong();
        accessCurrent.skip(-8).writeLong(0);
        // update the data for the parent
        splitInsertInParent(accessFather, current, right, maxLeft);
    }

    /**
     * Splits a leaf node (that is not the root)
     *
     * @param accessFather  The access to the parent node
     * @param accessCurrent The access to the current node
     * @param current       The entry for the node to split
     * @param count         The number of keys in the current node
     */
    private void splitLeaf(Access accessFather, Access accessCurrent, long current, char count) {
        // number of keys to transfer to the right (not including the neighbour pointer)
        int countRight = (count == 2 * N) ? N : N + 1;
        accessCurrent.seek(NODE_HEADER + N * CHILD_SIZE);
        long right = store.allocate(NODE_SIZE);
        long rightFirstKey = 0;
        try (Access accessRight = store.access(right, true)) {
            accessRight.writeLong(current);
            accessRight.writeChar(NODE_IS_LEAF);
            accessRight.writeChar((char) countRight);
            // write the transferred key-values and the neighbour pointer
            for (int i = 0; i != countRight + 1; i++) {
                long key = accessCurrent.readLong();
                long value = accessCurrent.readLong();
                if (i == 0)
                    rightFirstKey = key;
                accessRight.writeLong(key);
                accessRight.writeLong(value);
            }
            // write the leaf neighbour
            accessRight.writeLong(accessCurrent.readLong());
            accessRight.writeLong(accessCurrent.readLong());
        }
        // update the data for the split node
        accessCurrent.seek(8 + 2).writeChar((char) N);
        accessCurrent.seek(NODE_HEADER + N * CHILD_SIZE);
        // write the new leaf neighbour
        accessCurrent.writeLong(0);
        accessCurrent.writeLong(right);
        // update the data for the parent
        splitInsertInParent(accessFather, current, right, rightFirstKey);
    }

    /**
     * During a split, insert the data for a split node
     *
     * @param access        The access to the parent of the split node
     * @param leftNode      The entry for the split node in the left
     * @param rightNode     The entry for the split node on the right
     * @param rightFirstKey The first key of the split node on the right
     */
    private void splitInsertInParent(Access access, long leftNode, long rightNode, long rightFirstKey) {
        char count = access.seek(8 + 2).readChar();
        // find the index where to insert
        int insertAt = count;
        long originalKey = 0;
        for (int i = 0; i != count; i++) {
            long key = access.readLong();
            long value = access.readLong();
            if (value == leftNode) {
                insertAt = i;
                originalKey = key;
                break;
            }
        }
        // move the data on the right
        for (int i = count; i != insertAt; i--) {
            access.seek(NODE_HEADER + i * CHILD_SIZE);
            long entryKey = access.readLong();
            long entryValue = access.readLong();
            access.writeLong(entryKey);
            access.writeLong(entryValue);
        }
        // write the new pointers
        access.seek(NODE_HEADER + insertAt * CHILD_SIZE);
        access.writeLong(rightFirstKey);
        access.writeLong(leftNode);
        access.writeLong(originalKey);
        access.writeLong(rightNode);
        // update count
        count++;
        access.seek(8 + 2).writeChar(count);
    }

    /**
     * Tries to merge the left and right nodes
     * +---------------+---------+---------+--------+---------+
     * |  Left \ Right |  &lt;N  |   =N    |  =N+1  | &gt;N+1 |
     * +-e-------------+---------+---------+--------+---------+
     * | &lt;N         | Merge   |  Merge  | toLeft | toLeft  |
     * | =N            | Merge   |  Merge  |    .   | toLeft  |
     * | =N+1          | toRight |    .    |    .   |    .    |
     * | &gt;N+1       | toRight | toRight |    .   |    .    |
     * +---------------+---------+---------+------------------+
     *
     * @param father    The access for the father of both nodes
     * @param left      The left node
     * @param right     The right node
     * @param indexLeft The index of the left node in the father node
     * @return Whether the right node shall be freed
     */
    private boolean tryMergeInternals(Access father, Access left, Access right, int indexLeft) {
        int countLeft = left.seek(8 + 2).readChar();
        int countRight = right.seek(8 + 2).readChar();
        if (countLeft < N) {
            if (countRight <= N) {
                // merge left and right in left
                doMergeInternals(father, left, right, indexLeft, countLeft, countRight);
                return true;
            } else {
                doTransferToLeftInternal(father, left, right, indexLeft, countLeft, countRight);
            }
        } else if (countLeft == N) {
            if (countRight <= N) {
                doMergeInternals(father, left, right, indexLeft, countLeft, countRight);
                return true;
            } else if (countRight > N + 1) {
                doTransferToLeftInternal(father, left, right, indexLeft, countLeft, countRight);
            }
        } else if (countLeft == N + 1) {
            if (countRight < N) {
                doTransferToRightInternal(father, left, right, indexLeft, countLeft, countRight);
            }
        } else {
            doTransferToRightInternal(father, left, right, indexLeft, countLeft, countRight);
        }
        return false;
    }

    /**
     * Merges the left and right internal nodes into the left
     *
     * @param father     The access for the father of both nodes
     * @param left       The left node
     * @param right      The right node
     * @param indexLeft  The index of the left node in the father node
     * @param countLeft  The number of keys in the left node
     * @param countRight The number of keys in the right node
     */
    private void doMergeInternals(Access father, Access left, Access right, int indexLeft, int countLeft, int countRight) {
        left.seek(8 + 2).writeChar((char) (countLeft + countRight + 1));
        // use the key for the left node in the father as the key for the remainder on the left
        long leftKey = father.seek(NODE_HEADER + indexLeft * CHILD_SIZE).readLong();
        left.seek(NODE_HEADER + countLeft * CHILD_SIZE).writeLong(leftKey);
        left.skip(8);
        right.seek(NODE_HEADER);
        for (int i = 0; i != countRight + 1; i++) {
            left.writeLong(right.readLong());
            left.writeLong(right.readLong());
        }
        // update the father header
        doMergeUpdateFather(father, indexLeft);
    }

    /**
     * Transfers keys from the right internal node to the left internal node
     *
     * @param father     The access for the father of both nodes
     * @param left       The left node
     * @param right      The right node
     * @param indexLeft  The index of the left node in the father node
     * @param countLeft  The number of keys in the left node
     * @param countRight The number of keys in the right node
     */
    private void doTransferToLeftInternal(Access father, Access left, Access right, int indexLeft, int countLeft, int countRight) {
        int transferred = countRight - N;
        left.seek(8 + 2).writeChar((char) (countLeft + transferred));
        right.seek(8 + 2).writeChar((char) (countRight - transferred));
        // use the key for the left node in the father as the key for the remainder on the left
        long leftKey = father.seek(NODE_HEADER + indexLeft * CHILD_SIZE).readLong();
        left.seek(NODE_HEADER + countLeft * CHILD_SIZE).writeLong(leftKey);
        left.skip(8);
        right.seek(NODE_HEADER);
        for (int i = 0; i != transferred - 1; i++) {
            left.writeLong(right.readLong());
            left.writeLong(right.readLong());
        }
        // write key for the new remainder
        left.seek(NODE_HEADER + (countLeft + transferred) * CHILD_SIZE).writeLong(0);
        // repack the right node
        long firstKey = 0;
        for (int i = transferred; i != countRight + 1; i++) {
            right.seek(NODE_HEADER + i * CHILD_SIZE);
            long key = right.readLong();
            long value = right.readLong();
            if (i == transferred)
                firstKey = key;
            right.seek(NODE_HEADER + (i - transferred) * CHILD_SIZE);
            right.writeLong(key);
            right.writeLong(value);
        }
        // update the father
        father.seek(NODE_HEADER + indexLeft * CHILD_SIZE).writeLong(firstKey);
    }

    /**
     * Transfers keys from the left internal node to the right internal node
     *
     * @param father     The access for the father of both nodes
     * @param left       The left node
     * @param right      The right node
     * @param indexLeft  The index of the left node in the father node
     * @param countLeft  The number of keys in the left node
     * @param countRight The number of keys in the right node
     */
    private void doTransferToRightInternal(Access father, Access left, Access right, int indexLeft, int countLeft, int countRight) {
        int transferred = countLeft - N;
        left.seek(8 + 2).writeChar((char) (countLeft - transferred));
        right.seek(8 + 2).writeChar((char) (countRight + transferred));
        // move the current data in the right node
        for (int i = countRight; i != -1; i--) {
            right.seek(NODE_HEADER + i * CHILD_SIZE);
            long key = right.readLong();
            long value = right.readLong();
            right.seek(NODE_HEADER + (i + transferred) * CHILD_SIZE);
            right.writeLong(key);
            right.writeLong(value);
        }
        // use the key for the left node in the father as the key for the remainder on the left
        long leftKey = father.seek(NODE_HEADER + indexLeft * CHILD_SIZE).readLong();
        left.seek(NODE_HEADER + countLeft * CHILD_SIZE).writeLong(leftKey);
        // transfer
        left.seek(NODE_HEADER + (countLeft - transferred + 1) * CHILD_SIZE);
        right.seek(NODE_HEADER);
        long firstKey = 0;
        for (int i = 0; i != transferred; i++) {
            long key = left.readLong();
            if (i == 0)
                firstKey = 0;
            right.writeLong(key);
            right.writeLong(left.readLong());
        }
        // write key for the new remainder
        left.seek(NODE_HEADER + (countLeft - transferred) * CHILD_SIZE).writeLong(0);
        // update the father
        father.seek(NODE_HEADER + indexLeft * CHILD_SIZE).writeLong(firstKey);
    }

    /**
     * Tries to merge the left and right leaf nodes
     * +---------------+---------+---------+--------+---------+
     * |  Left \ Right |  &lt;N  |   =N    |  =N+1  | &gt;N+1 |
     * +-e-------------+---------+---------+--------+---------+
     * | &lt;N         | Merge   |  Merge  | toLeft | toLeft  |
     * | =N            | Merge   |  Merge  |    .   | toLeft  |
     * | =N+1          | toRight |    .    |    .   |    .    |
     * | &gt;N+1       | toRight | toRight |    .   |    .    |
     * +---------------+---------+---------+------------------+
     *
     * @param father         The access for the father of both nodes
     * @param left           The left node
     * @param right          The right node
     * @param indexLeft      The index of the left node in the father node
     * @param neighbourRight The entry for the right node
     * @return Whether the right node shall be freed
     */
    private boolean tryMergeLeaves(Access father, Access left, Access right, int indexLeft, long neighbourRight) {
        int countLeft = left.seek(8 + 2).readChar();
        int countRight = right.seek(8 + 2).readChar();
        if (countLeft < N) {
            if (countRight <= N) {
                // merge left and right in left
                doMergeLeaves(father, left, right, indexLeft, countLeft, countRight);
                return true;
            } else {
                doTransferToLeftLeaf(father, left, right, indexLeft, neighbourRight, countLeft, countRight);
            }
        } else if (countLeft == N) {
            if (countRight <= N) {
                doMergeLeaves(father, left, right, indexLeft, countLeft, countRight);
                return true;
            } else if (countRight > N + 1) {
                doTransferToLeftLeaf(father, left, right, indexLeft, neighbourRight, countLeft, countRight);
            }
        } else if (countLeft == N + 1) {
            if (countRight < N) {
                doTransferToRightLeaf(father, left, right, indexLeft, neighbourRight, countLeft, countRight);
            }
        } else {
            doTransferToRightLeaf(father, left, right, indexLeft, neighbourRight, countLeft, countRight);
        }
        return false;
    }

    /**
     * Merges the left and right leaf nodes into the left
     *
     * @param father     The access for the father of both nodes
     * @param left       The left node
     * @param right      The right node
     * @param indexLeft  The index of the left node in the father node
     * @param countLeft  The number of keys in the left node
     * @param countRight The number of keys in the right node
     */
    private void doMergeLeaves(Access father, Access left, Access right, int indexLeft, int countLeft, int countRight) {
        left.seek(8 + 2).writeChar((char) (countLeft + countRight));
        left.seek(NODE_HEADER + countLeft * CHILD_SIZE);
        right.seek(NODE_HEADER);
        for (int i = 0; i != countRight + 1; i++) {
            left.writeLong(right.readLong());
            left.writeLong(right.readLong());
        }
        // update the father header
        doMergeUpdateFather(father, indexLeft);
    }

    /**
     * Transfers keys from the right leaf node to the left leaf node
     *
     * @param father         The access for the father of both nodes
     * @param left           The left node
     * @param right          The right node
     * @param indexLeft      The index of the left node in the father node
     * @param neighbourRight The entry for the right node
     * @param countLeft      The number of keys in the left node
     * @param countRight     The number of keys in the right node
     */
    private void doTransferToLeftLeaf(Access father, Access left, Access right, int indexLeft, long neighbourRight, int countLeft, int countRight) {
        int transferred = countRight - N;
        left.seek(8 + 2).writeChar((char) (countLeft + transferred));
        right.seek(8 + 2).writeChar((char) (countRight - transferred));
        left.seek(NODE_HEADER + countLeft * CHILD_SIZE);
        right.seek(NODE_HEADER);
        for (int i = 0; i != transferred; i++) {
            left.writeLong(right.readLong());
            left.writeLong(right.readLong());
        }
        left.writeLong(0);
        left.writeLong(neighbourRight);
        // repack the right node
        long firstKey = 0;
        for (int i = transferred; i != countRight + 1; i++) {
            right.seek(NODE_HEADER + i * CHILD_SIZE);
            long key = right.readLong();
            long value = right.readLong();
            if (i == transferred)
                firstKey = key;
            right.seek(NODE_HEADER + (i - transferred) * CHILD_SIZE);
            right.writeLong(key);
            right.writeLong(value);
        }
        // update the father
        father.seek(NODE_HEADER + indexLeft * CHILD_SIZE).writeLong(firstKey);
    }

    /**
     * Transfers keys from the left leaf node to the right leaf node
     *
     * @param father         The access for the father of both nodes
     * @param left           The left node
     * @param right          The right node
     * @param indexLeft      The index of the left node in the father node
     * @param neighbourRight The entry for the right node
     * @param countLeft      The number of keys in the left node
     * @param countRight     The number of keys in the right node
     */
    private void doTransferToRightLeaf(Access father, Access left, Access right, int indexLeft, long neighbourRight, int countLeft, int countRight) {
        int transferred = countLeft - N;
        left.seek(8 + 2).writeChar((char) (countLeft - transferred));
        right.seek(8 + 2).writeChar((char) (countRight + transferred));
        // move the current data in the right node
        for (int i = countRight; i != -1; i--) {
            right.seek(NODE_HEADER + i * CHILD_SIZE);
            long key = right.readLong();
            long value = right.readLong();
            right.seek(NODE_HEADER + (i + transferred) * CHILD_SIZE);
            right.writeLong(key);
            right.writeLong(value);
        }
        // transfer
        left.seek(NODE_HEADER + (countLeft - transferred) * CHILD_SIZE);
        right.seek(NODE_HEADER);
        long firstKey = 0;
        for (int i = 0; i != transferred; i++) {
            long key = left.readLong();
            if (i == 0)
                firstKey = 0;
            right.writeLong(key);
            right.writeLong(left.readLong());
        }
        left.seek(NODE_HEADER + (countLeft - transferred) * CHILD_SIZE);
        left.writeLong(0);
        left.writeLong(neighbourRight);
        father.seek(NODE_HEADER + indexLeft * CHILD_SIZE).writeLong(firstKey);
    }

    /**
     * Merges the left and right leaf nodes
     *
     * @param father    The access for the father of both nodes
     * @param indexLeft The index of the left node in the father node
     */
    private void doMergeUpdateFather(Access father, int indexLeft) {
        int countFather = father.seek(8 + 2).readChar();
        // the father node loses a key
        father.seek(8 + 2).writeChar((char) (countFather - 1));
        // shift the keys and pointers
        long keyRight = father.seek(NODE_HEADER + (indexLeft + 1) * CHILD_SIZE).readLong();
        father.seek(NODE_HEADER + indexLeft * CHILD_SIZE).writeLong(keyRight);
        for (int i = indexLeft + 2; i < countFather + 2; i++) {
            father.seek(NODE_HEADER + i * CHILD_SIZE);
            long key = father.readLong();
            long value = father.readLong();
            father.seek(NODE_HEADER + (i - 1) * CHILD_SIZE);
            father.writeLong(key);
            father.writeLong(value);
        }
    }

    /**
     * The stack for visiting the nodes in the tree
     */
    private static class Stack {
        /**
         * The stack's items
         */
        public long[] items;
        /**
         * The stack's head (index of the top item)
         * -1 when the stack is empty
         */
        public int head;

        /**
         * Initializes this stack
         */
        public Stack() {
            this.items = new long[CHILD_COUNT * 2];
            this.head = -1;
        }

        /**
         * Gets whether the stack is empty
         *
         * @return Whether the stack is empty
         */
        public boolean isEmpty() {
            return head == -1;
        }

        /**
         * Pops the stack's head
         *
         * @return The top item
         */
        public long pop() {
            return items[head--];
        }

        /**
         * Pushes the children of the current node represented by its access to the specified stack
         *
         * @param accessCurrent The access to the current node
         */
        private void pushChildren(Access accessCurrent) {
            char count = accessCurrent.seek(8 + 2).readChar();
            while (head + count + 1 >= items.length) {
                items = Arrays.copyOf(items, items.length + CHILD_COUNT);
            }
            for (int i = 0; i != count; i++) {
                items[++head] = accessCurrent.skip(8).readLong();
            }
            long value = accessCurrent.skip(8).readLong();
            if (value != Constants.KEY_NULL)
                items[++head] = value;
        }
    }

    /**
     * Removes all entries from this map
     */
    public void clear() {
        clear(null);
    }

    /**
     * Removes all entries from this map
     *
     * @param entries The buffer for removed entries
     */
    public void clear(List<Entry> entries) {
        Stack stack = new Stack();
        try (Access accessHead = store.access(head, true)) {
            // initializes the stack with the content of the head
            clearOnNode(accessHead, stack, entries);
            // deletes the tree, starting with nodes on the stack
            while (!stack.isEmpty()) {
                long current = stack.pop();
                try (Access accessCurrent = store.access(current, false)) {
                    clearOnNode(accessCurrent, stack, entries);
                }
                store.free(current);
            }
            // rewrite the head
            accessHead.seek(8);
            accessHead.writeChar(NODE_IS_LEAF);
            accessHead.writeChar((char) 0);
            accessHead.writeLong(0);
            accessHead.writeLong(Constants.KEY_NULL);
        }
    }

    /**
     * Inspects the B+ tree node represented by its access
     *
     * @param accessCurrent The access to the current node
     * @param stack         The stack of nodes to inspect
     * @param entries       The buffer of entries, if required
     */
    private void clearOnNode(Access accessCurrent, Stack stack, List<Entry> entries) {
        boolean isLeaf = accessCurrent.seek(8).readChar() == NODE_IS_LEAF;
        if (isLeaf) {
            if (entries != null) {
                char count = accessCurrent.readChar();
                for (int i = 0; i != count; i++) {
                    entries.add(new Entry(
                            accessCurrent.readLong(),
                            accessCurrent.readLong()
                    ));
                }
            }
        } else {
            stack.pushChildren(accessCurrent);
        }
    }

    /**
     * Gets an iterator over the entries in this map
     *
     * @return An iterator over the entries
     */
    public Iterator<Entry> entries() {
        return new EntriesIterator();
    }

    /**
     * Implements an iterator over the entries in the B+ tree
     */
    private class EntriesIterator implements Iterator<Entry> {
        /**
         * The keys in the current node
         */
        private final long[] currentKeys;
        /**
         * The values in the current node
         */
        private final long[] currentValues;
        /**
         * The current number of key-value mappings in the current node
         */
        private int currentCount;
        /**
         * The neighbour of the current node
         */
        private long currentNeighbour;
        /**
         * The next index in the current node
         */
        private int nextIndex;
        /**
         * The result structure
         */
        private final Entry result;

        /**
         * Initializes this iterator
         */
        public EntriesIterator() {
            this.currentKeys = new long[CHILD_COUNT];
            this.currentValues = new long[CHILD_COUNT];
            this.currentCount = 0;
            this.currentNeighbour = Constants.KEY_NULL;
            this.nextIndex = 0;
            this.result = new Entry(0, 0);
            findLeafNode();
        }

        /**
         * Finds the first leaf node for the tree
         */
        private void findLeafNode() {
            Access accessFather = null;
            Access accessCurrent = store.access(head, false);
            try {
                while (true) {
                    // inspect the current node
                    boolean isLeaf = accessCurrent.skip(8).readChar() == NODE_IS_LEAF;
                    if (isLeaf) {
                        loadLeafNode(accessCurrent);
                        break;
                    } else {
                        long next = accessCurrent.seek(NODE_HEADER + 8).readLong();
                        // free the father if any, rotate the accesses and access the next node
                        if (accessFather != null)
                            accessFather.close();
                        accessFather = accessCurrent;
                        accessCurrent = store.access(next, false);
                    }
                }
            } finally {
                if (accessFather != null)
                    accessFather.close();
                if (accessCurrent != null)
                    accessCurrent.close();
            }
        }

        /**
         * Loads the data for a leaf node
         *
         * @param access The access to the leaf node
         */
        private void loadLeafNode(Access access) {
            currentCount = access.seek(8 + 2).readChar();
            for (int i = 0; i != currentCount; i++) {
                currentKeys[i] = access.readLong();
                currentValues[i] = access.readLong();
            }
            nextIndex = 0;
            currentNeighbour = access.skip(8).readLong();
        }

        @Override
        public boolean hasNext() {
            return nextIndex >= 0 && nextIndex < currentCount;
        }

        @Override
        public Entry next() {
            result.reset(currentKeys[nextIndex], currentValues[nextIndex]);
            nextIndex++;
            if (nextIndex >= currentCount && currentNeighbour != Constants.KEY_NULL) {
                // go to next node
                try (Access access = store.access(currentNeighbour, false)) {
                    loadLeafNode(access);
                }
            }
            return result;
        }

        @Override
        public void remove() {
            // try to remove the mapping
            // this may fail if the map was modified
            tryRemove(result.key, result.value);
        }
    }
}
