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
import fr.cenotelie.commons.utils.ByteUtils;

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
class RawFileBlock extends Endpoint {
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
     * Zeroes some part of this page
     *
     * @param from The starting index within this page (included)
     * @param to   The last index within this page (excluded)
     */
    public void zeroes(int from, int to) {
        Arrays.fill(buffer, from, to, (byte) 0);
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
        return ByteUtils.getChar(buffer, i);
    }

    @Override
    public short readShort(long index) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        return ByteUtils.getShort(buffer, i);
    }

    @Override
    public int readInt(long index) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        return ByteUtils.getInt(buffer, i);
    }

    @Override
    public long readLong(long index) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        return ByteUtils.getLong(buffer, i);
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
        ByteUtils.setChar(buffer, i, value);
        parent.onWriteUpTo(index + 2);
        isDirty = true;
    }

    @Override
    public void writeShort(long index, short value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        ByteUtils.setShort(buffer, i, value);
        parent.onWriteUpTo(index + 2);
        isDirty = true;
    }

    @Override
    public void writeInt(long index, int value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        ByteUtils.setInt(buffer, i, value);
        parent.onWriteUpTo(index + 4);
        isDirty = true;
    }

    @Override
    public void writeLong(long index, long value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        ByteUtils.setLong(buffer, i, value);
        parent.onWriteUpTo(index + 8);
        isDirty = true;
    }
}
