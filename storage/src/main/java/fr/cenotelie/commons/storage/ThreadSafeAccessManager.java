/*******************************************************************************
 * Copyright (c) 2016 Association Cénotélie (cenotelie.fr)
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

import fr.cenotelie.commons.utils.ByteUtils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Manages the concurrent accesses onto a single storage system so that
 * no writing thread can overlap with other threads accessing the same storage system.
 * This structure is thread safe and lock-free.
 *
 * @author Laurent Wouters
 */
class ThreadSafeAccessManager {
    /**
     * The size of the access pool
     */
    private static final int ACCESSES_POOL_SIZE = 64;
    /**
     * The index of the head of the list of active accesses
     */
    private static final int ACTIVE_HEAD_ID = 0;
    /**
     * The index of the tail of the list of active accesses
     */
    private static final int ACTIVE_TAIL_ID = 1;
    /**
     * The maximum number of concurrent threads
     */
    private static final int THREAD_POOL_SIZE = 16;

    /**
     * The operation succeeded
     */
    private static final int RESULT_OK = 0;
    /**
     * The operation failed because the list was stale
     */
    private static final int RESULT_FAILURE_STALE = 1;
    /**
     * The operation failed because there was a write overlap
     */
    private static final int RESULT_FAILURE_OVERLAP = 2;
    /**
     * The operation failed because there was a concurrent edit
     */
    private static final int RESULT_FAILURE_CONCURRENT = 3;

    /**
     * The storage system that is protected by this manager
     */
    private final Storage storage;
    /**
     * The pool of existing accesses in the manager
     */
    private final ThreadSafeAccess[] accesses;
    /**
     * The current number of accesses in the pool
     */
    private final AtomicInteger accessesCount;
    /**
     * The state of the accesses managed by this structure
     * The state of an access is composed of:
     * - int: key (access location)
     * - byte: access state (0=free, 1=active, 2=logically removed, 3=returning)
     * - byte: when returning: bit-field for remaining touching threads
     * - byte: when returning: bit-field for remaining touching threads
     * - byte: the index of the next active access
     */
    private final AtomicLongArray accessesState;
    /**
     * The bit-field of current threads touching the list
     */
    private final AtomicInteger accessesThreads;
    /**
     * The pool of free threads identifiers
     */
    private final AtomicInteger threads;

    /**
     * Initializes this manager
     *
     * @param storage The storage system that is protected by this manager
     */
    public ThreadSafeAccessManager(Storage storage) {
        this.storage = storage;
        this.accesses = new ThreadSafeAccess[ACCESSES_POOL_SIZE];
        this.accesses[ACTIVE_HEAD_ID] = new ThreadSafeAccess(this, ACTIVE_HEAD_ID, 0);
        this.accesses[ACTIVE_TAIL_ID] = new ThreadSafeAccess(this, ACTIVE_TAIL_ID, Integer.MAX_VALUE);
        this.accessesCount = new AtomicInteger(2);
        this.accessesState = new AtomicLongArray(ACCESSES_POOL_SIZE);
        this.accessesState.set(ACTIVE_HEAD_ID, 0x0000000001000001L);
        this.accessesState.set(ACTIVE_TAIL_ID, 0x7FFFFFFF01000001L);
        this.accessesThreads = new AtomicInteger(0);
        this.threads = new AtomicInteger(0x00FFFF);
    }

    /**
     * Gets the key for an access in the specified state
     *
     * @param state The state of an access
     * @return The key for the access
     */
    private static int stateKey(long state) {
        return (int) (state >>> 32);
    }

    /**
     * Gets whether the state of an access indicates that the access is free
     *
     * @param state The state of an access
     * @return Whether the access is free
     */
    private static boolean stateIsFree(long state) {
        return (state & 0x00000000FF000000L) == 0x0000000000000000L;
    }

    /**
     * Gets whether the state of an access indicates that the access is active
     *
     * @param state The state of an access
     * @return Whether the access is active
     */
    private static boolean stateIsActive(long state) {
        return (state & 0x00000000FF000000L) == 0x0000000001000000;
    }

    /**
     * Gets whether the state of an access indicates that the access is being returned to the free access list
     *
     * @param state The state of an access
     * @return Whether the access is being returned
     */
    private static boolean stateIsReturning(long state) {
        return (state & 0x00000000FF000000L) == 0x0000000003000000;
    }

    /**
     * When returning an access, gets the threads that can touch this access
     *
     * @param state The state of an access
     * @return The threads that can touch this access
     */
    private static int stateThreads(long state) {
        return (int) ((state & 0x0000000000FFFF00L) >>> 8);
    }

    /**
     * Gets the index of the next active access
     *
     * @param state The state of an access
     * @return The index of the next active access
     */
    private static int stateActiveNext(long state) {
        return (int) (state & 0x00000000000000FFL);
    }

    /**
     * Gets an active state for an access with the specified key
     *
     * @param key The new key for the access
     * @return The new state
     */
    private static long stateSetActive(int key) {
        return ByteUtils.uLong(key) << 32 | 0x0000000001000000L;
    }

    /**
     * Gets the marked version of the state for the specified one
     *
     * @param state The initial state of an access
     * @return The equivalent marked state
     */
    private static long stateSetLogicallyRemoved(long state) {
        return (state & 0xFFFFFFFF00FFFFFFL) | 0x0000000002000000L;
    }

    /**
     * Gets the version of the state representing a returning access
     *
     * @param state          The initial state of an access
     * @param currentThreads The current threads accessing the active list
     * @return The equivalent returning state
     */
    private static long stateSetReturning(long state, int currentThreads) {
        return (state & 0xFFFFFFFF000000FFL) | 0x0000000003000000L | ByteUtils.uLong(currentThreads) << 8;
    }

    /**
     * Gets the version of the state representing a free access
     *
     * @param state The initial state of an access
     * @return The equivalent free state
     */
    private static long stateSetFree(long state) {
        return (state & 0xFFFFFFFF00FFFFFFL);
    }

    /**
     * Gets the new state when removing a thread as potentially touching the access
     *
     * @param state            The initial state of an access
     * @param threadIdentifier The identifier of the thread to remove
     * @return The new state
     */
    private static long stateRemoveThread(long state, int threadIdentifier) {
        return state & ~(ByteUtils.uLong(threadIdentifier) << 8);
    }

    /**
     * Gets the new state with a new value for the next active access
     *
     * @param state The initial state of an access
     * @param next  The index of the next active access
     * @return The new state
     */
    private static long stateSetNextActive(long state, int next) {
        return (state & 0xFFFFFFFFFFFFFF00L) | ByteUtils.uLong(next);
    }

    /**
     * Gets a identifier for this thread
     *
     * @return The identifier for this thread
     */
    private int getThreadId() {
        while (true) {
            int mask = threads.get();
            for (int i = 0; i != THREAD_POOL_SIZE; i++) {
                int id = (1 << i);
                if ((mask & id) == id) {
                    // the identifier is available
                    int newMask = mask & ~id;
                    if (threads.compareAndSet(mask, newMask))
                        // reserved the identifier
                        return id;
                    // the mask has changed
                    break;
                }
            }
        }
    }

    /**
     * Returns a thread identifier when no longer required
     *
     * @param threadIdentifier The identifier to return
     */
    private void returnThreadId(int threadIdentifier) {
        while (true) {
            int mask = threads.get();
            if (threads.compareAndSet(mask, mask | threadIdentifier))
                return;
        }
    }

    /**
     * Registers the specified thread as accessing the list of active accesses
     *
     * @param threadIdentifier The identifier of this thread
     */
    private void beginActiveAccess(int threadIdentifier) {
        while (true) {
            int mask = accessesThreads.get();
            if (mask == -1)
                // the access is locked out
                continue;
            if (accessesThreads.compareAndSet(mask, mask | threadIdentifier))
                return;
        }
    }

    /**
     * Unregisters the specified thread as accessing the list of active accesses
     *
     * @param threadIdentifier The identifier of this thread
     */
    private void endActiveAccess(int threadIdentifier) {
        while (true) {
            int mask = accessesThreads.get();
            if (mask == -1)
                // the access is locked out
                continue;
            if (accessesThreads.compareAndSet(mask, mask & ~threadIdentifier))
                return;
        }
    }

    /**
     * Prevents the registering and un-registering of threads accessing the list of active accesses
     *
     * @return The current threads on the list of active accesses
     */
    private int lockActiveAccesses() {
        while (true) {
            int mask = accessesThreads.get();
            if (mask == -1)
                // the access is locked out
                continue;
            if (accessesThreads.compareAndSet(mask, -1))
                return mask;
        }
    }

    /**
     * Re-enable the registering and un-registering of threads accessing the list of active accesses
     *
     * @param threads The current threads on the list of active accesses
     */
    private void unlockActiveAccesses(int threads) {
        accessesThreads.compareAndSet(-1, threads);
    }

    /**
     * Searches the left and right node in the list for a place to insert the specified access and tries to insert the specified access
     *
     * @param toInsert The access to be inserted
     * @param key      The key to insert at
     * @return The result for this operation
     */
    private int listSearchAndInsert(int toInsert, int key) {
        ThreadSafeAccess accessToInsert = accesses[toInsert];

        // find the left node
        int leftNode;
        long leftNodeState;
        int rightNode;
        int currentNode = ACTIVE_HEAD_ID;
        long currentNodeState = accessesState.get(currentNode);
        while (true) {
            leftNode = currentNode;
            leftNodeState = currentNodeState;
            currentNode = stateActiveNext(currentNodeState);
            currentNodeState = accessesState.get(currentNode);
            if (!stateIsActive(currentNodeState))
                return RESULT_FAILURE_STALE;
            ThreadSafeAccess accessCurrentNode = accesses[currentNode];
            if ((accessToInsert.isWritable() || accessCurrentNode.isWritable()) && !accessToInsert.disjoints(accessCurrentNode))
                // there is a write overlap
                return RESULT_FAILURE_OVERLAP;
            if (key < stateKey(currentNodeState))
                break;
        }
        rightNode = currentNode;

        // look for overlap after the insertion point
        while (true) {
            currentNode = stateActiveNext(currentNodeState);
            currentNodeState = accessesState.get(currentNode);
            if (!stateIsActive(currentNodeState))
                return RESULT_FAILURE_STALE;
            if (stateKey(currentNodeState) >= key + accessToInsert.getLength())
                break;
            ThreadSafeAccess accessCurrentNode = accesses[currentNode];
            if ((accessToInsert.isWritable() || accessCurrentNode.isWritable()) && !accessToInsert.disjoints(accessCurrentNode))
                // there is a write overlap
                return RESULT_FAILURE_OVERLAP;
        }

        // setup the access to insert
        long toInsertState = accessesState.get(toInsert);
        accessesState.set(toInsert, stateSetNextActive(toInsertState, rightNode));
        // try to insert
        if (accessesState.compareAndSet(leftNode, leftNodeState, stateSetNextActive(leftNodeState, toInsert)))
            return RESULT_OK;
        return RESULT_FAILURE_CONCURRENT;
    }

    /**
     * Inserts an access into the list of live accesses
     * The method returns only when the access is safely inserted, i.e. there is no blocking access
     *
     * @param toInsert The access to be inserted
     * @param key      The key to insert at
     */
    private void listInsert(int toInsert, int key) {
        while (true) {
            // gets a thread identifier for the current thread
            int threadIdentifier = getThreadId();
            // register this thread as accessing the list
            beginActiveAccess(threadIdentifier);
            try {
                int result = listSearchAndInsert(toInsert, key);
                if (result == RESULT_OK)
                    return;
            } finally {
                // unregisters this thread as accessing the list
                endActiveAccess(threadIdentifier);
                // participates to the pool cleanup effort
                poolCleanup(threadIdentifier);
                // release the thread identifier
                returnThreadId(threadIdentifier);
            }
        }
    }

    /**
     * Searches the left and right node in the list for the access to be removed and try to mark it as removed
     *
     * @param toRemove The access to be removed
     * @return The result for this operation
     */
    private int listSearchAndRemove(int toRemove) {
        // find the left node
        int leftNode;
        long leftNodeState;
        int currentNode = ACTIVE_HEAD_ID;
        long currentNodeState = accessesState.get(currentNode);
        while (true) {
            leftNode = currentNode;
            leftNodeState = currentNodeState;
            currentNode = stateActiveNext(currentNodeState);
            if (currentNode == toRemove)
                break;
            currentNodeState = accessesState.get(currentNode);
            if (!stateIsActive(currentNodeState))
                return RESULT_FAILURE_STALE;
        }

        // mark as logically deleted
        long oldState = accessesState.get(toRemove);
        long newState = stateSetLogicallyRemoved(oldState);
        if (!accessesState.compareAndSet(toRemove, oldState, newState))
            return RESULT_FAILURE_CONCURRENT;
        oldState = newState;

        // try to remove from the list
        if (!accessesState.compareAndSet(leftNode, leftNodeState, stateSetNextActive(leftNodeState, stateActiveNext(oldState))))
            return RESULT_FAILURE_CONCURRENT;

        // no-longer reachable from the list's head
        // mark the access as returning provided the clearance of current threads
        int currentThreads = lockActiveAccesses();
        newState = stateSetReturning(oldState, currentThreads);
        accessesState.compareAndSet(toRemove, oldState, newState);
        unlockActiveAccesses(currentThreads);
        return RESULT_OK;
    }

    /**
     * Removes an access from the list of live accesses
     *
     * @param toRemove The access to be removed
     */
    private void listRemove(int toRemove) {
        while (true) {
            // gets a thread identifier for the current thread
            int threadIdentifier = getThreadId();
            // register this thread as accessing the list
            beginActiveAccess(threadIdentifier);
            try {
                int result = listSearchAndRemove(toRemove);
                if (result == RESULT_OK)
                    return;
            } finally {
                // unregisters this thread as accessing the list
                endActiveAccess(threadIdentifier);
                // participates to the pool cleanup effort
                poolCleanup(threadIdentifier);
                // release the thread identifier
                returnThreadId(threadIdentifier);
            }
        }
    }

    /**
     * Gets an access to the associated storage system for the specified span
     *
     * @param location The location of the span within the storage system
     * @param length   The length of the allowed span
     * @param writable Whether the access allows writing
     * @return The new access, or null if it cannot be obtained
     */
    public Access get(int location, int length, boolean writable) {
        if (location < 0 || length <= 0)
            throw new IllegalArgumentException();
        ThreadSafeAccess access = newAccess(location);
        access.setup(storage, location, length, writable);
        listInsert(access.identifier, location);
        return access;
    }

    /**
     * Ends an access to the storage system
     *
     * @param access The access
     */
    void onAccessEnd(ThreadSafeAccess access) {
        listRemove(access.identifier);
    }

    /**
     * Resolves a free access object
     *
     * @param key The key for the access
     * @return A free access object
     */
    private ThreadSafeAccess newAccess(int key) {
        int count = accessesCount.get();
        while (count < ACCESSES_POOL_SIZE) {
            // the pool is not full, try to grow it
            if (accessesCount.compareAndSet(count, count + 1)) {
                accesses[count] = new ThreadSafeAccess(this, count);
                accessesState.set(count, stateSetActive(key));
                return accesses[count];
            }
            count = accessesCount.get();
        }

        // the pool is full
        while (true) {
            for (int i = 2; i != ACCESSES_POOL_SIZE; i++) {
                long state = accessesState.get(i);
                if (stateIsFree(state)) {
                    long newState = stateSetActive(key);
                    if (accessesState.compareAndSet(i, state, newState))
                        return accesses[i];
                }
            }
        }
    }

    /**
     * Collects the returning accesses that are no longer touched by a thread
     *
     * @param threadIdentifier The identifier of this thread
     */
    private void poolCleanup(int threadIdentifier) {
        int count = accessesCount.get();
        for (int i = 2; i < count; i++) {
            long state = accessesState.get(i);
            if (stateIsReturning(state) && (stateThreads(state) & threadIdentifier) == threadIdentifier)
                onAccessReturning(threadIdentifier, i, state);
        }
    }

    /**
     * When a returning access touched by this thread is found while cleaning up the pool
     *
     * @param threadIdentifier The identifier of this thread
     * @param accessIdentifier The identifier of the access
     * @param currentState     The supposed current state of the access
     */
    private void onAccessReturning(int threadIdentifier, int accessIdentifier, long currentState) {
        // remove the mark for the current thread
        while (true) {
            long newState = stateRemoveThread(currentState, threadIdentifier);
            boolean success = accessesState.compareAndSet(accessIdentifier, currentState, newState);
            if (success) {
                currentState = newState;
                break;
            }
            currentState = accessesState.get(accessIdentifier);
        }
        if (stateThreads(currentState) != 0)
            // more threads can still touch this access
            return;
        // no more thread can touch this node, return the access as free again
        accessesState.set(accessIdentifier, stateSetFree(currentState));
    }
}
