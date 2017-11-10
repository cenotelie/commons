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

package fr.cenotelie.commons.storage.files;

import fr.cenotelie.commons.storage.Constants;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a block of contiguous data in a file that is managed in a thread-safe manner
 * This structure uses lock-free mechanisms for thread safety and uses a state-machine to track its use:
 * <p>
 * +--------------+------------------------------------------------------------+
 * | State        | Description                                                |
 * +--------------+------------------------------------------------------------+
 * | Free         | The block is not assigned to a location in the file        |
 * | Reserved     | The block is reserved for a location and being initialized |
 * | Reclaiming   | The block is begin reclaimed for reuse                     |
 * | Ready        | The block is ready for read and write operations           |
 * | InUse(n)     | The block is used for IO operations by n threads           |
 * +--------------+------------------------------------------------------------+
 * <p>
 * +------------------+--------------------------------------+
 * | Operation        | Transition from state to state       |
 * +------------------+--------------------------------------+
 * | reserve:         | Free --&gt; Reserved --&gt; Ready    |
 * | use:             | Ready        --&gt; InUse(1)         |
 * | use:             | InUse(n)     --&gt; InUse(n+1)       |
 * | release:         | InUse(n)     --&gt; InUse(n-1)       |
 * | release:         | InUse(1)     --&gt; Ready            |
 * | reclaim:         | Ready --&gt; Reclaiming --&gt; Ready |
 * | flush:           | Ready --&gt; Reclaiming --&gt; Ready |
 * +------------------+--------------------------------------+
 *
 * @author Laurent Wouters
 */
class RawFileBlockTS extends RawFileBlock {
    /**
     * The block is free, i.e. not assigned to any location
     */
    private static final int BLOCK_STATE_FREE = 0;
    /**
     * The block is reserved, i.e. is going to contain data but is not ready yet
     */
    private static final int BLOCK_STATE_RESERVED = 1;
    /**
     * The block is begin reclaimed for reuse
     */
    private static final int BLOCK_STATE_RECLAIMING = 2;
    /**
     * The block exists and is ready for IO
     */
    private static final int BLOCK_STATE_READY = 3;
    /**
     * The block is used by one or more users in a shared manner
     */
    private static final int BLOCK_STATE_IN_USE = 4;

    /**
     * When reserving the block, the block was free and successfully reserved
     */
    public static final int RESERVE_RESULT_OK = 0;
    /**
     * When reserving the block, the block was already reserved for the same location
     */
    public static final int RESERVE_RESULT_READY = 1;
    /**
     * When reserving the block, the block was already reserved for another location
     */
    public static final int RESERVE_RESULT_FAIL = -1;

    /**
     * Gets the name of the block state
     *
     * @param state The block state
     * @return The associated name
     */
    private static String stateName(int state) {
        if (state < 0)
            return "INVALID(" + state + ")";
        switch (state) {
            case BLOCK_STATE_FREE:
                return "READY";
            case BLOCK_STATE_RESERVED:
                return "RESERVED";
            case BLOCK_STATE_READY:
                return "READY";
            case BLOCK_STATE_RECLAIMING:
                return "RECLAIMING";
            case BLOCK_STATE_IN_USE:
                return "IN_USE(1)";
            default:
                return "IN_USE(" + (state - BLOCK_STATE_READY) + ")";
        }
    }

    /**
     * The state of this block
     */
    private final AtomicInteger state;

    /**
     * Initializes this structure
     */
    public RawFileBlockTS() {
        this.state = new AtomicInteger(BLOCK_STATE_FREE);
    }

    /**
     * Tries to reserve this block
     *
     * @param location The location for the block
     * @param channel  The originating file channel
     * @param fileSize The current size of the file
     * @param time     The current time
     * @return The reservation status
     */
    public int reserve(long location, FileChannel channel, long fileSize, long time) {
        while (true) {
            int current = state.get();
            if (current >= BLOCK_STATE_READY) {
                // the block was made ready by another thread
                if (this.location != location)
                    return RESERVE_RESULT_FAIL;
                touch(time);
                return RESERVE_RESULT_READY;
            }
            if (current == BLOCK_STATE_RESERVED) {
                if (this.location != location)
                    return RESERVE_RESULT_FAIL;
                // wait for the block to be ready
                continue;
            }
            if (current == BLOCK_STATE_FREE) {
                // the block is free
                if (!state.compareAndSet(BLOCK_STATE_FREE, BLOCK_STATE_RESERVED))
                    // woops, too late
                    continue;
                // the block was free and is now reserved
                doSetup(location, channel, fileSize, time);
                state.compareAndSet(BLOCK_STATE_RESERVED, BLOCK_STATE_READY);
                return RESERVE_RESULT_OK;
            }
        }
    }

    /**
     * Initializes this block for use
     *
     * @param location The location for the block
     * @param channel  The originating file channel
     * @param fileSize The current size of the file
     * @param time     The current time
     */
    private void doSetup(long location, FileChannel channel, long fileSize, long time) {
        this.location = location;
        if (this.buffer == null)
            this.buffer = ByteBuffer.allocate(Constants.PAGE_SIZE);
        this.isDirty = false;
        if (this.location < fileSize) {
            try {
                load(channel);
            } catch (IOException exception) {
                state.set(BLOCK_STATE_READY);
                throw new Error("Failed to read block at 0x" + Long.toHexString(location), exception);
            }
        } else {
            zeroes();
        }
        touch(time);
    }

    /**
     * Attempt to get a shared use of this block for the specified expected location
     *
     * @param location The expected location for this block
     * @param time     The current time
     * @return true if the shared use is granted, false if the effective location of this block was not the expected one
     */
    public boolean use(long location, long time) {
        while (true) {
            int current = state.get();
            switch (current) {
                case BLOCK_STATE_FREE:
                    throw new Error("Bad block state: " + stateName(current) + ", expected READY");
                case BLOCK_STATE_RESERVED:
                    if (this.location != location)
                        return false;
                    break;
                case BLOCK_STATE_RECLAIMING:
                    return false;
                case BLOCK_STATE_READY: {
                    if (state.compareAndSet(BLOCK_STATE_READY, BLOCK_STATE_IN_USE)) {
                        if (this.location == location) {
                            touch(time);
                            return true;
                        }
                        // this is the wrong location, release the block and fail
                        release();
                        return false;
                    }
                    break;
                }
                default: {
                    if (state.compareAndSet(current, current + 1)) {
                        if (this.location == location) {
                            touch(time);
                            return true;
                        }
                        // this is the wrong location, release the block and fail
                        release();
                        return false;
                    }
                    break;
                }
            }
        }
    }

    /**
     * Releases a share use of this block
     */
    public void release() {
        while (true) {
            int current = state.get();
            if (current <= BLOCK_STATE_READY)
                throw new Error("Bad block state: " + stateName(current) + ", expected IN_USE");
            else if (current == BLOCK_STATE_IN_USE && state.compareAndSet(BLOCK_STATE_IN_USE, BLOCK_STATE_READY))
                return;
            else if (current > BLOCK_STATE_IN_USE && state.compareAndSet(current, current - 1))
                return;
        }
    }

    /**
     * Tries to reclaims this block for another location
     *
     * @param location The location for the block
     * @param channel  The originating file channel
     * @param fileSize The current size of the file
     * @param time     The current time
     * @return Whether the block was reclaimed
     */
    public boolean reclaim(long location, FileChannel channel, long fileSize, long time) {
        if (!state.compareAndSet(BLOCK_STATE_READY, BLOCK_STATE_RECLAIMING))
            return false;
        try {
            serialize(channel);
        } catch (IOException exception) {
            state.set(BLOCK_STATE_READY);
            throw new Error("Failed to write block at 0x" + Long.toHexString(location), exception);
        }
        doSetup(location, channel, fileSize, time);
        state.set(BLOCK_STATE_READY);
        return true;
    }

    /**
     * Flushes any outstanding changes to the backend file
     *
     * @param channel The originating file channel
     */
    public void flush(FileChannel channel) {
        if (!isDirty)
            // not dirty at this time, do nothing
            return;
        while (true) {
            if (state.compareAndSet(BLOCK_STATE_READY, BLOCK_STATE_RECLAIMING))
                break;
        }
        try {
            serialize(channel);
        } catch (IOException exception) {
            throw new Error("Failed to write block at 0x" + Long.toHexString(location), exception);
        } finally {
            state.set(BLOCK_STATE_READY);
        }
    }
}
