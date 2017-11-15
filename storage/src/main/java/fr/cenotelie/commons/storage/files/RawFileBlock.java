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
    protected byte[] buffer;
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
        ByteBuffer temp = ByteBuffer.wrap(buffer, 0, length);
        int total = 0;
        while (total < length) {
            int read = channel.read(temp, location + total);
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
        ByteBuffer temp = ByteBuffer.wrap(buffer, 0, length);
        int total = 0;
        while (total < length) {
            int written = channel.write(temp, location + total);
            total += written;
        }
        isDirty = false;
    }

    /**
     * Zeroes the end of this page from the specified index forward
     *
     * @param index The starting index within this page
     */
    public void zeroesFrom(int index) {
        Arrays.fill(buffer, index, Constants.PAGE_SIZE - index, (byte) 0);
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
        return buffer[(int) (index & Constants.INDEX_MASK_LOWER)];
    }

    @Override
    public byte[] readBytes(long index, int length) {
        byte[] result = new byte[length];
        readBytes(index, result, 0, length);
        return result;
    }

    @Override
    public void readBytes(long index, byte[] buffer, int start, int length) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        System.arraycopy(this.buffer, i, buffer, start, length);
    }

    @Override
    public char readChar(long index) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        return (char) (((int) buffer[i] << 8)
                | ((int) buffer[i + 1]));
    }

    @Override
    public short readShort(long index) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        return (short) (((int) buffer[i] << 8)
                | ((int) buffer[i + 1]));
    }

    @Override
    public int readInt(long index) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        return (((int) buffer[i] << 24)
                | ((int) buffer[i + 1] << 16)
                | ((int) buffer[i + 2] << 8)
                | ((int) buffer[i + 3]));
    }

    @Override
    public long readLong(long index) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        return (((long) buffer[i] << 56)
                | ((long) buffer[i + 1] << 48)
                | ((long) buffer[i + 2] << 40)
                | ((long) buffer[i + 3] << 32)
                | ((long) buffer[i + 4] << 24)
                | ((long) buffer[i + 5] << 16)
                | ((long) buffer[i + 6] << 8)
                | ((long) buffer[i + 7]));
    }

    @Override
    public float readFloat(long index) {
        return Float.intBitsToFloat(readInt(index));
    }

    @Override
    public double readDouble(long index) {
        return Double.longBitsToDouble(readLong(index));
    }

    @Override
    public void writeByte(long index, byte value) {
        buffer[(int) (index & Constants.INDEX_MASK_LOWER)] = value;
        parent.onWriteUpTo(index + 1);
        isDirty = true;
    }

    @Override
    public void writeBytes(long index, byte[] value) {
        writeBytes(index, value, 0, value.length);
    }

    @Override
    public void writeBytes(long index, byte[] buffer, int start, int length) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        System.arraycopy(buffer, start, this.buffer, i, length);
        parent.onWriteUpTo(index + length);
        isDirty = true;
    }

    @Override
    public void writeChar(long index, char value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        buffer[i] = (byte) (value >>> 8 & 0xFF);
        buffer[i + 1] = (byte) (value & 0xFF);
        parent.onWriteUpTo(index + 2);
        isDirty = true;
    }

    @Override
    public void writeShort(long index, short value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        buffer[i] = (byte) (value >>> 8 & 0xFF);
        buffer[i + 1] = (byte) (value & 0xFF);
        parent.onWriteUpTo(index + 2);
        isDirty = true;
    }

    @Override
    public void writeInt(long index, int value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        buffer[i] = (byte) (value >>> 24 & 0xFF);
        buffer[i + 1] = (byte) (value >>> 16 & 0xFF);
        buffer[i + 2] = (byte) (value >>> 8 & 0xFF);
        buffer[i + 3] = (byte) (value & 0xFF);
        parent.onWriteUpTo(index + 4);
        isDirty = true;
    }

    @Override
    public void writeLong(long index, long value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        buffer[i] = (byte) (value >>> 56 & 0xFF);
        buffer[i + 1] = (byte) (value >>> 48 & 0xFF);
        buffer[i + 2] = (byte) (value >>> 40 & 0xFF);
        buffer[i + 3] = (byte) (value >>> 32 & 0xFF);
        buffer[i + 4] = (byte) (value >>> 24 & 0xFF);
        buffer[i + 5] = (byte) (value >>> 16 & 0xFF);
        buffer[i + 6] = (byte) (value >>> 8 & 0xFF);
        buffer[i + 7] = (byte) (value & 0xFF);
        parent.onWriteUpTo(index + 8);
        isDirty = true;
    }

    @Override
    public void writeFloat(long index, float value) {
        writeInt(index, Float.floatToIntBits(value));
    }

    @Override
    public void writeDouble(long index, double value) {
        writeLong(index, Double.doubleToLongBits(value));
    }
}
