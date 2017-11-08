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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * Represents a block of contiguous data in a file
 * This class is not thread safe
 *
 * @author Laurent Wouters
 */
class RawFileBlock implements AutoCloseable, IOEndpoint {
    /**
     * The number of bits to use in order to represent an index within a block
     */
    public static final int BLOCK_INDEX_LENGTH = 13;
    /**
     * The size of a block in bytes
     */
    public static final int BLOCK_SIZE = 1 << BLOCK_INDEX_LENGTH;
    /**
     * The mask for the index within a block
     */
    public static final int INDEX_MASK_LOWER = BLOCK_SIZE - 1;

    /**
     * The associated buffer
     */
    protected ByteBuffer buffer;
    /**
     * The location of this block in the parent file
     */
    protected long location;
    /**
     * The timestamp for the last time this block was hit
     */
    protected long lastHit;
    /**
     * Whether this block is dirty
     */
    protected boolean isDirty;

    /**
     * Gets the location of this block in the parent file
     *
     * @return The location of this block in the parent file
     */
    public long getLocation() {
        return location;
    }

    /**
     * Gets the timestamp for the last time this block was hit
     *
     * @return The timestamp for the last time this block was hit
     */
    public long getLastHit() {
        return lastHit;
    }

    /**
     * Initializes this structure
     */
    public RawFileBlock() {
        this.buffer = null;
        this.location = -1;
        this.lastHit = Long.MIN_VALUE;
        this.isDirty = false;
    }

    /**
     * Touches this block
     *
     * @param time The current time
     */
    protected void touch(long time) {
        lastHit = Math.max(lastHit, time);
    }

    /**
     * Loads this block using the specified file channel
     *
     * @param channel The file channel to read from
     * @throws IOException When an IO error occurs
     */
    protected void load(FileChannel channel) throws IOException {
        int total = 0;
        buffer.position(0);
        while (total < BLOCK_SIZE) {
            int read = channel.read(buffer, location + total);
            if (read == -1)
                throw new IOException("Unexpected end of stream");
            total += read;
        }
    }

    /**
     * Serializes this block to the underlying file channel
     *
     * @param channel The originating file channel
     * @throws IOException When an IO error occurs
     */
    protected void serialize(FileChannel channel) throws IOException {
        if (isDirty) {
            buffer.position(0);
            int total = 0;
            while (total < BLOCK_SIZE) {
                int written = channel.write(buffer, location + total);
                total += written;
            }
            isDirty = false;
        }
    }

    /**
     * Arrays of empty data used for zeroing the content of a buffer
     */
    private static final byte[] ZEROES = new byte[256];

    /**
     * Zeroes the content of the buffer
     */
    protected void zeroes() {
        if (buffer.hasArray()) {
            Arrays.fill(buffer.array(), (byte) 0);
            return;
        }
        buffer.position(0);
        for (int i = 0; i != BLOCK_SIZE; i += ZEROES.length) {
            buffer.put(ZEROES);
        }
    }

    @Override
    public byte readByte(int index) {
        return buffer.get(index & INDEX_MASK_LOWER);
    }

    @Override
    public byte[] readBytes(int index, int length) {
        byte[] result = new byte[length];
        readBytes(index, result, 0, length);
        return result;
    }

    @Override
    public synchronized void readBytes(int index, byte[] buffer, int start, int length) {
        this.buffer.position(index & INDEX_MASK_LOWER);
        this.buffer.get(buffer, start, length);
    }

    @Override
    public char readChar(int index) {
        return buffer.getChar(index & INDEX_MASK_LOWER);
    }

    @Override
    public int readInt(int index) {
        return buffer.getInt(index & INDEX_MASK_LOWER);
    }

    @Override
    public long readLong(int index) {
        return buffer.getLong(index & INDEX_MASK_LOWER);
    }

    @Override
    public float readFloat(int index) {
        return buffer.getFloat(index & INDEX_MASK_LOWER);
    }

    @Override
    public double readDouble(int index) {
        return buffer.getDouble(index & INDEX_MASK_LOWER);
    }

    @Override
    public void writeByte(int index, byte value) {
        buffer.put(index & INDEX_MASK_LOWER, value);
        isDirty = true;
    }

    @Override
    public void writeBytes(int index, byte[] value) {
        writeBytes(index, value, 0, value.length);
    }

    @Override
    public void writeBytes(int index, byte[] buffer, int start, int length) {
        this.buffer.position(index & INDEX_MASK_LOWER);
        this.buffer.put(buffer, start, length);
        isDirty = true;
    }

    @Override
    public void writeChar(int index, char value) {
        buffer.putChar(index & INDEX_MASK_LOWER, value);
        isDirty = true;
    }

    @Override
    public void writeInt(int index, int value) {
        buffer.putInt(index & INDEX_MASK_LOWER, value);
        isDirty = true;
    }

    @Override
    public void writeLong(int index, long value) {
        buffer.putLong(index & INDEX_MASK_LOWER, value);
        isDirty = true;
    }

    @Override
    public void writeFloat(int index, float value) {
        buffer.putFloat(index & INDEX_MASK_LOWER, value);
        isDirty = true;
    }

    @Override
    public void writeDouble(int index, double value) {
        buffer.putDouble(index & INDEX_MASK_LOWER, value);
        isDirty = true;
    }

    @Override
    public void close() {
    }
}
