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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Manages the concurrent accesses onto a single IO backend
 * This structure is thread safe and lock-free.
 *
 * @author Laurent Wouters
 */
public class TSAccessManager {
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
     * The backend element that is protected by this manager
     */
    private final StorageBackend backend;
    /**
     * The pool of existing accesses in the manager
     */
    private final TSAccess[] accesses;
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
     * Initializes this pool
     *
     * @param backend The backend element that is protected by this manager
     */
    public TSAccessManager(StorageBackend backend) {
        this.backend = backend;
        this.accesses = new TSAccess[ACCESSES_POOL_SIZE];
        this.accesses[ACTIVE_HEAD_ID] = new TSAccess(this, ACTIVE_HEAD_ID, 0);
        this.accesses[ACTIVE_TAIL_ID] = new TSAccess(this, ACTIVE_TAIL_ID, Integer.MAX_VALUE);
        this.accessesCount = new AtomicInteger(2);
        this.accessesState = new AtomicLongArray(ACCESSES_POOL_SIZE);
        this.accessesState.set(ACTIVE_HEAD_ID, 0x0000000001000001L);
        this.accessesState.set(ACTIVE_TAIL_ID, 0x7FFFFFFF01000001L);
        this.accessesThreads = new AtomicInteger(0);
        this.threads = new AtomicInteger(0x00FFFF);
    }

    /**
     * Unsigned promotion of an integer to a long
     * Replacement for Integer.toUnsignedLong when using JDK 7
     *
     * @param i The integer
     * @return The resulting long
     */
    private static long ul(int i) {
        return (long) i & 0xFFFFFFFFL;
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
        return ul(key) << 32 | 0x0000000001000000L;
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
        return (state & 0xFFFFFFFF000000FFL) | 0x0000000003000000L | ul(currentThreads) << 8;
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
        return state & ~(ul(threadIdentifier) << 8);
    }

    /**
     * Gets the new state with a new value for the next active access
     *
     * @param state The initial state of an access
     * @param next  The index of the next active access
     * @return The new state
     */
    private static long stateSetNextActive(long state, int next) {
        return (state & 0xFFFFFFFFFFFFFF00L) | ul(next);
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
     * @return Whether the attempt is successful
     */
    private boolean listSearchAndInsert(int toInsert, int key) {
        TSAccess accessToInsert = accesses[toInsert];

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
                return false;
            TSAccess accessCurrentNode = accesses[currentNode];
            if ((accessToInsert.isWritable() || accessCurrentNode.isWritable()) && !accessToInsert.disjoints(accessCurrentNode))
                // there is a write overlap
                return false;
            if (key < stateKey(currentNodeState))
                break;
        }
        rightNode = currentNode;

        // look for overlap after the insertion point
        while (true) {
            currentNode = stateActiveNext(currentNodeState);
            currentNodeState = accessesState.get(currentNode);
            if (!stateIsActive(currentNodeState))
                return false;
            if (stateKey(currentNodeState) >= key + accessToInsert.getLength())
                break;
            TSAccess accessCurrentNode = accesses[currentNode];
            if ((accessToInsert.isWritable() || accessCurrentNode.isWritable()) && !accessToInsert.disjoints(accessCurrentNode))
                // there is a write overlap
                return false;
        }

        // setup the access to insert
        long toInsertState = accessesState.get(toInsert);
        accessesState.set(toInsert, stateSetNextActive(toInsertState, rightNode));
        // try to insert
        return (accessesState.compareAndSet(leftNode, leftNodeState, stateSetNextActive(leftNodeState, toInsert)));
    }

    /**
     * Inserts an access into the list of live accesses
     * The method returns only when the access is safely inserted, i.e. there is no blocking access
     *
     * @param toInsert The access to be inserted
     * @param key      The key to insert at
     */
    private void listInsert(int toInsert, int key) {
        int threadIdentifier = getThreadId();
        while (true) {
            beginActiveAccess(threadIdentifier);
            boolean success = listSearchAndInsert(toInsert, key);
            endActiveAccess(threadIdentifier);
            poolCleanup(threadIdentifier);
            if (success)
                break;
        }
        returnThreadId(threadIdentifier);
    }

    /**
     * Searches the left and right node in the list for the access to be removed and try to mark it as removed
     *
     * @param toRemove The access to be removed
     * @return Whether the attempt is successful
     */
    private boolean listSearchAndRemove(int toRemove) {
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
                return false;
        }

        // mark as logically deleted
        long oldState = accessesState.get(toRemove);
        long newState = stateSetLogicallyRemoved(oldState);
        if (!accessesState.compareAndSet(toRemove, oldState, newState))
            return false;
        oldState = newState;

        // try to remove from the list
        if (!accessesState.compareAndSet(leftNode, leftNodeState, stateSetNextActive(leftNodeState, stateActiveNext(oldState))))
            return false;

        // no-longer reachable from the list's head
        // mark the access as returning provided the clearance of current threads
        int currentThreads = lockActiveAccesses();
        newState = stateSetReturning(oldState, currentThreads);
        accessesState.compareAndSet(toRemove, oldState, newState);
        unlockActiveAccesses(currentThreads);
        return true;
    }

    /**
     * Removes an access from the list of live accesses
     *
     * @param toRemove The access to be removed
     */
    private void listRemove(int toRemove) {
        int threadIdentifier = getThreadId();
        while (true) {
            beginActiveAccess(threadIdentifier);
            boolean success = listSearchAndRemove(toRemove);
            endActiveAccess(threadIdentifier);
            poolCleanup(threadIdentifier);
            if (success)
                break;
        }
        returnThreadId(threadIdentifier);
    }

    /**
     * Gets an access to the associated backend for the specified span
     *
     * @param location The location of the span within the backend
     * @param length   The length of the allowed span
     * @param writable Whether the access allows writing
     * @return The new access, or null if it cannot be obtained
     */
    public StorageAccess get(int location, int length, boolean writable) {
        TSAccess access = newAccess(location);
        access.setup(backend, location, length, writable);
        listInsert(access.identifier, location);
        return access;
    }

    /**
     * Ends an access to the backend
     *
     * @param access The access
     */
    void onAccessEnd(TSAccess access) {
        listRemove(access.identifier);
    }

    /**
     * Resolves a free access object
     *
     * @param key The key for the access
     * @return A free access object
     */
    private TSAccess newAccess(int key) {
        int count = accessesCount.get();
        while (count < ACCESSES_POOL_SIZE) {
            // the pool is not full, try to grow it
            if (accessesCount.compareAndSet(count, count + 1)) {
                accesses[count] = new TSAccess(this, count);
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
