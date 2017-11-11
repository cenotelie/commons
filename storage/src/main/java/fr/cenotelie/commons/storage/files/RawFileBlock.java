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
import fr.cenotelie.commons.storage.StorageEndpoint;

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
class RawFileBlock extends StorageEndpoint {
    /**
     * The parent file
     */
    protected final RawFileBuffered parent;
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
     *
     * @param parent The parent file
     */
    public RawFileBlock(RawFileBuffered parent) {
        this.parent = parent;
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
     * @param length  The number of bytes to read
     * @throws IOException When an IO error occurs
     */
    protected void load(FileChannel channel, int length) throws IOException {
        if (length == Constants.PAGE_SIZE) {
            loadBuffer(buffer, channel);
        } else {
            ByteBuffer temp = ByteBuffer.allocate(length);
            loadBuffer(temp, channel);
            System.arraycopy(temp.array(), 0, buffer.array(), 0, length);
        }
    }

    /**
     * Loads the specified buffer with data from a file channel
     *
     * @param buffer  The byte buffer to load
     * @param channel The file channel to read from
     * @throws IOException When an IO error occurs
     */
    private void loadBuffer(ByteBuffer buffer, FileChannel channel) throws IOException {
        int total = 0;
        buffer.position(0);
        while (total < buffer.limit()) {
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
     * @param length  The number of bytes to write
     * @throws IOException When an IO error occurs
     */
    protected void serialize(FileChannel channel, int length) throws IOException {
        if (!isDirty)
            return;
        if (length == Constants.PAGE_SIZE) {
            serializeBuffer(buffer, channel);
        } else {
            ByteBuffer temp = ByteBuffer.allocate(length);
            System.arraycopy(buffer.array(), 0, temp.array(), 0, length);
            serializeBuffer(temp, channel);
        }
        isDirty = false;
    }

    /**
     * Serializes the specified buffer to a file channel
     *
     * @param buffer  The byte buffer to serialize
     * @param channel The file channel to write to
     * @throws IOException When an IO error occurs
     */
    protected void serializeBuffer(ByteBuffer buffer, FileChannel channel) throws IOException {
        buffer.position(0);
        int total = 0;
        while (total < buffer.limit()) {
            int written = channel.write(buffer, location + total);
            total += written;
        }
    }

    /**
     * Zeroes the end of this page from the specified index forward
     *
     * @param index The starting index within this page
     */
    public void zeroesFrom(int index) {
        Arrays.fill(buffer.array(), index, Constants.PAGE_SIZE - index, (byte) 0);
    }

    @Override
    public long getIndexLowerBound() {
        return location;
    }

    @Override
    public long getIndexUpperBound() {
        return location + Constants.PAGE_SIZE;
    }

    @Override
    public byte readByte(long index) {
        return buffer.get((int) (index & Constants.INDEX_MASK_LOWER));
    }

    @Override
    public byte[] readBytes(long index, int length) {
        byte[] result = new byte[length];
        readBytes(index, result, 0, length);
        return result;
    }

    @Override
    public synchronized void readBytes(long index, byte[] buffer, int start, int length) {
        this.buffer.position((int) (index & Constants.INDEX_MASK_LOWER));
        this.buffer.get(buffer, start, length);
    }

    @Override
    public char readChar(long index) {
        return buffer.getChar((int) (index & Constants.INDEX_MASK_LOWER));
    }

    @Override
    public short readShort(long index) {
        return buffer.getShort((int) (index & Constants.INDEX_MASK_LOWER));
    }

    @Override
    public int readInt(long index) {
        return buffer.getInt((int) (index & Constants.INDEX_MASK_LOWER));
    }

    @Override
    public long readLong(long index) {
        return buffer.getLong((int) (index & Constants.INDEX_MASK_LOWER));
    }

    @Override
    public float readFloat(long index) {
        return buffer.getFloat((int) (index & Constants.INDEX_MASK_LOWER));
    }

    @Override
    public double readDouble(long index) {
        return buffer.getDouble((int) (index & Constants.INDEX_MASK_LOWER));
    }

    @Override
    public void writeByte(long index, byte value) {
        buffer.put((int) (index & Constants.INDEX_MASK_LOWER), value);
        parent.onWriteUpTo(index + 1);
        isDirty = true;
    }

    @Override
    public void writeBytes(long index, byte[] value) {
        writeBytes(index, value, 0, value.length);
    }

    @Override
    public void writeBytes(long index, byte[] buffer, int start, int length) {
        this.buffer.position((int) (index & Constants.INDEX_MASK_LOWER));
        this.buffer.put(buffer, start, length);
        parent.onWriteUpTo(index + length);
        isDirty = true;
    }

    @Override
    public void writeChar(long index, char value) {
        buffer.putChar((int) (index & Constants.INDEX_MASK_LOWER), value);
        parent.onWriteUpTo(index + 2);
        isDirty = true;
    }

    @Override
    public void writeShort(long index, short value) {
        buffer.putShort((int) (index & Constants.INDEX_MASK_LOWER), value);
        parent.onWriteUpTo(index + 2);
        isDirty = true;
    }

    @Override
    public void writeInt(long index, int value) {
        buffer.putInt((int) (index & Constants.INDEX_MASK_LOWER), value);
        parent.onWriteUpTo(index + 4);
        isDirty = true;
    }

    @Override
    public void writeLong(long index, long value) {
        buffer.putLong((int) (index & Constants.INDEX_MASK_LOWER), value);
        parent.onWriteUpTo(index + 8);
        isDirty = true;
    }

    @Override
    public void writeFloat(long index, float value) {
        buffer.putFloat((int) (index & Constants.INDEX_MASK_LOWER), value);
        parent.onWriteUpTo(index + 4);
        isDirty = true;
    }

    @Override
    public void writeDouble(long index, double value) {
        buffer.putDouble((int) (index & Constants.INDEX_MASK_LOWER), value);
        parent.onWriteUpTo(index + 8);
        isDirty = true;
    }
}
