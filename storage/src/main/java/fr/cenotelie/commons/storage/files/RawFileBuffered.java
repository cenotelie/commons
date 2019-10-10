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
import fr.cenotelie.commons.storage.Endpoint;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a single file for storage with buffered reading and writing
 * This structure is thread-safe in the way it manages its inner data.
 * However, it does not ensure that multiple thread do not overlap while reading and writing to locations.
 *
 * @author Laurent Wouters
 */
public class RawFileBuffered extends RawFile {
    /**
     * The maximum number of loaded blocks
     */
    private static final int FILE_MAX_LOADED_BLOCKS = 1024;

    /**
     * The storage system is ready for IO
     */
    private static final int STATE_READY = 0;
    /**
     * The storage system is currently busy with an operation
     */
    private static final int STATE_BUSY = 1;
    /**
     * The file is now closed
     */
    private static final int STATE_CLOSED = -1;

    /**
     * The accessed file
     */
    private final File file;
    /**
     * Whether the access is writable
     */
    private final boolean writable;
    /**
     * The file channel
     */
    private final FileChannel channel;
    /**
     * The loaded blocks in this file
     */
    private final RawFileBlockTS[] blocks;
    /**
     * The number of currently loaded blocks
     */
    private final AtomicInteger blockCount;
    /**
     * The total size of this file
     */
    private final AtomicLong size;
    /**
     * The current time
     */
    private final AtomicLong time;
    /**
     * The state of this file storage system
     */
    private final AtomicInteger state;

    /**
     * Initializes this data file
     *
     * @param file     The file location
     * @param writable Whether the file can be written to
     * @throws IOException When the file cannot be accessed
     */
    public RawFileBuffered(File file, boolean writable) throws IOException {
        this.file = file;
        this.writable = writable;
        this.channel = newChannel(file, writable);
        this.blocks = new RawFileBlockTS[FILE_MAX_LOADED_BLOCKS];
        for (int i = 0; i != FILE_MAX_LOADED_BLOCKS; i++)
            this.blocks[i] = new RawFileBlockTS(this);
        this.blockCount = new AtomicInteger(0);
        this.size = new AtomicLong(initSize());
        this.time = new AtomicLong(Long.MIN_VALUE + 1);
        this.state = new AtomicInteger(STATE_READY);
    }

    /**
     * Get the file channel for this file
     *
     * @param file     The file location
     * @param writable Whether the file can be written to
     * @return The file channel
     * @throws IOException When the file cannot be accessed
     */
    private static FileChannel newChannel(File file, boolean writable) throws IOException {
        if (file.exists() && !file.canWrite())
            writable = false;
        return !writable
                ? FileChannel.open(file.toPath(), StandardOpenOption.READ)
                : FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    /**
     * Gets the current size of the file channel
     *
     * @return The current size
     * @throws IOException When the file cannot be accessed
     */
    private long initSize() throws IOException {
        return channel.size();
    }

    /**
     * Ticks the time of this file
     *
     * @return The new time
     */
    private long tick() {
        return time.incrementAndGet();
    }

    @Override
    public File getSystemFile() {
        return file;
    }

    @Override
    public boolean isWritable() {
        return writable;
    }

    @Override
    public long getSize() {
        return size.get();
    }

    @Override
    public boolean cut(long from, long to) throws IOException {
        if (from < 0 || from > to)
            throw new IndexOutOfBoundsException();
        if (from == to)
            // 0-length cut => do nothing
            return false;

        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new IllegalStateException();
            if (!state.compareAndSet(STATE_READY, STATE_BUSY))
                continue;
            try {
                // do we have to update the current size?
                while (true) {
                    long currentSize = size.get();
                    if (from >= currentSize)
                        // start after the current size => no effect
                        return false;
                    if (to < currentSize) {
                        // no effect on the current size ...
                        cutOverwrite(from, to);
                        cutCleanBlocks(from, to);
                        return true;
                    }
                    if (size.compareAndSet(currentSize, from)) {
                        // truncate
                        channel.truncate(from);
                        cutCleanBlocks(from, currentSize);
                        return true;
                    }
                }
            } finally {
                state.set(STATE_READY);
            }
        }
    }

    /**
     * When cutting the file, overwrite the physical file
     *
     * @param from The starting index to cut at (included)
     * @param to   The end index to cut to (excluded)
     * @throws IOException When an IO error occurred
     */
    private void cutOverwrite(long from, long to) throws IOException {
        long current = from;
        int remaining = (int) (to - from);
        ByteBuffer buffer = null;
        while (remaining >= 1024) {
            if (buffer == null)
                buffer = ByteBuffer.allocate(1024);
            cutOverwriteWithBuffer(buffer, current);
            remaining -= 1024;
            current += 1024;
        }
        if (remaining > 0) {
            buffer = ByteBuffer.allocate(remaining);
            cutOverwriteWithBuffer(buffer, current);
        }
    }

    /**
     * When cutting the file, overwrite the physical file using the specified buffer
     *
     * @param buffer   The buffer to use
     * @param location The location to write to
     * @throws IOException When an IO error occurred
     */
    private void cutOverwriteWithBuffer(ByteBuffer buffer, long location) throws IOException {
        buffer.position(0);
        int total = 0;
        while (total < buffer.capacity()) {
            int written = channel.write(buffer, location + total);
            total += written;
        }
    }

    /**
     * When cutting the file, clean-up relevant blocks
     *
     * @param from The starting index to cut at (included)
     * @param to   The end index to cut to (excluded)
     */
    private void cutCleanBlocks(long from, long to) {
        int fromPage = (int) (from >>> Constants.PAGE_INDEX_LENGTH);
        int fromIndex = (int) (from & Constants.INDEX_MASK_LOWER);
        int toPage = (int) (to >>> Constants.PAGE_INDEX_LENGTH);
        int toIndex = (int) (to & Constants.INDEX_MASK_LOWER);
        if (toIndex == 0) {
            toPage--;
            toIndex = Constants.PAGE_SIZE;
        }

        for (int i = 0; i != blockCount.get(); i++) {
            if (blocks[i].location >= to || blocks[i].location + Constants.PAGE_SIZE <= from)
                // starts after the cut, or ends before the cut
                continue;
            int blockPage = (int) (blocks[i].location >>> Constants.PAGE_INDEX_LENGTH);
            if (blockPage == fromPage) {
                // this block contains the from index
                blocks[i].zeroes(fromIndex, blockPage == toPage ? toIndex : Constants.PAGE_SIZE);
            } else {
                blocks[i].zeroes(0, blockPage == toPage ? toIndex : Constants.PAGE_SIZE);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new IllegalStateException();
            if (!state.compareAndSet(STATE_READY, STATE_BUSY))
                continue;
            try {
                long currentSize = size.get();
                for (int i = 0; i != blockCount.get(); i++)
                    blocks[i].flush(channel, currentSize);
                channel.force(true);
            } finally {
                state.set(STATE_READY);
            }
            return;
        }
    }

    @Override
    public Endpoint acquireEndpointAt(long index) {
        if (index < 0)
            throw new IndexOutOfBoundsException();
        try {
            return getBlockFor(index);
        } catch (IOException exception) {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void releaseEndpoint(Endpoint endpoint) {
        ((RawFileBlockTS) endpoint).release();
    }

    @Override
    public void close() throws IOException {
        do {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new IllegalStateException();
        } while (!state.compareAndSet(STATE_READY, STATE_CLOSED));
        channel.close();
    }

    /**
     * Acquires the block for the specified index in this file
     * This method ensures that:
     * 1) Only one block object can be assigned to a location in the file
     * 2) When a block object is returned, it corresponds to the requested location
     * 3) ... and will continue to do so until the access finishes using it
     *
     * @param index The requested index in this file
     * @return The corresponding block
     * @throws IOException When an IO error occurred
     */
    private RawFileBlockTS getBlockFor(long index) throws IOException {
        long targetLocation = index & Constants.INDEX_MASK_UPPER;
        if (blockCount.get() < FILE_MAX_LOADED_BLOCKS)
            return getBlockWhenNotFull(targetLocation);
        else
            return getBlockWhenFull(targetLocation);
    }

    /**
     * Acquires the block for the specified index in this file when the pool of blocks is not full yet
     *
     * @param targetLocation The location of the requested block in this file
     * @return The corresponding block
     * @throws IOException When an IO error occurred
     */
    private RawFileBlockTS getBlockWhenNotFull(long targetLocation) throws IOException {
        // try to allocate one of the free block
        int count = blockCount.get();
        while (count < FILE_MAX_LOADED_BLOCKS) {
            // look for the block
            for (int i = 0; i != count; i++) {
                // is this the block we are looking for?
                if (blocks[i].getLocation() == targetLocation && blocks[i].use(targetLocation, tick())) {
                    // yes and we locked it
                    return blocks[i];
                }
            }
            // get the last block
            RawFileBlockTS target = blocks[count];
            // try to reserve it
            switch (target.reserve(targetLocation, channel, tick())) {
                case RawFileBlockTS.RESERVE_RESULT_READY:
                    // same block and location, but another thread ...
                    if (target.use(targetLocation, tick()))
                        return target;
                    break;
                case RawFileBlockTS.RESERVE_RESULT_OK:
                    // reserved by this thread for the location
                    blockCount.incrementAndGet();
                    if (target.use(targetLocation, tick()))
                        return target;
                    break;
            }
            // retry with the next block
            count = blockCount.get();
        }
        // now the pool if full ... fallback
        return getBlockWhenNotFound(targetLocation);
    }

    /**
     * Acquires the block for the specified index in this file when the pool of blocks is full
     * This method attempts a full scan of the allocated block to find the corresponding one.
     * If this fails, is falls back to the reclaiming an existing one.
     *
     * @param targetLocation The location of the requested block in this file
     * @return The corresponding block
     * @throws IOException When an IO error occurred
     */
    private RawFileBlockTS getBlockWhenFull(long targetLocation) throws IOException {
        for (int i = 0; i != FILE_MAX_LOADED_BLOCKS; i++) {
            // is this the block we are looking for?
            if (blocks[i].getLocation() == targetLocation && blocks[i].use(targetLocation, tick())) {
                // yes and we locked it
                return blocks[i];
            }
        }
        return getBlockWhenNotFound(targetLocation);
    }

    /**
     * Acquires the block for the specified index in this file when previous attempts to find a corresponding block failed
     * This method will reclaim the oldest block when an already provisioned block for the location is not found.
     *
     * @param targetLocation The location of the requested block in this file
     * @return The corresponding block
     * @throws IOException When an IO error occurred
     */
    private RawFileBlockTS getBlockWhenNotFound(long targetLocation) throws IOException {
        do {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new IllegalStateException();
        } while (!state.compareAndSet(STATE_READY, STATE_BUSY));

        // lookup and reclaim the oldest block
        while (true) {
            // keep track of the oldest block
            int oldestIndex = -1;
            long oldestTime = Long.MAX_VALUE;
            long oldestLocation = -1;
            for (int i = 0; i != FILE_MAX_LOADED_BLOCKS; i++) {
                // is this the block we are looking for?
                if (blocks[i].getLocation() == targetLocation && blocks[i].use(targetLocation, tick())) {
                    // yes and we locked it
                    state.set(STATE_READY);
                    return blocks[i];
                }
                // is this the oldest block?
                long t = blocks[i].getLastHit();
                if (t < oldestTime) {
                    oldestIndex = i;
                    oldestTime = t;
                    oldestLocation = blocks[i].getLocation();
                }
            }
            // we did not find the block, try to reclaim the oldest one
            RawFileBlockTS target = blocks[oldestIndex];
            try {
                if (target.getLastHit() == oldestTime // the time did not change
                        && target.getLocation() == oldestLocation // still the same location
                        && target.reclaim(targetLocation, channel, size.get(), tick()) // try to reclaim
                ) {
                    if (target.use(targetLocation, tick())) {
                        // we got the block
                        state.set(STATE_READY);
                        return target;
                    }
                }
            } finally {
                state.set(STATE_READY);
            }
        }
    }

    /**
     * When a thread wrote up to an index
     *
     * @param index The maximum index written to
     */
    void onWriteUpTo(long index) {
        while (true) {
            long current = size.get();
            if (current > index)
                // not the furthest => exit
                return;
            if (size.compareAndSet(current, index))
                // succeeded to update => exit
                return;
            // start over
        }
    }
}
