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

package fr.cenotelie.commons.storage.memory;

import fr.cenotelie.commons.storage.Constants;
import fr.cenotelie.commons.storage.StorageEndpoint;

import java.nio.ByteBuffer;

/**
 * Represents a page of data for an in-memory storage system
 *
 * @author Laurent Wouters
 */
class InMemoryPage extends StorageEndpoint {
    /**
     * The parent store
     */
    private final InMemoryStore store;
    /**
     * The location of the page
     */
    private final long location;
    /**
     * The page's content
     */
    private final ByteBuffer buffer;

    /**
     * Initializes this page
     *
     * @param store    The parent store
     * @param location The location of the page
     */
    public InMemoryPage(InMemoryStore store, long location) {
        this.store = store;
        this.location = location;
        this.buffer = ByteBuffer.allocate(Constants.PAGE_SIZE);
    }

    /**
     * Gets the location of the page within the backing system
     *
     * @return The location of the page within the backing system
     */
    public long getLocation() {
        return location;
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
        store.onWriteUpTo(index + 1);
    }

    @Override
    public void writeBytes(long index, byte[] value) {
        writeBytes(index, value, 0, value.length);
        store.onWriteUpTo(index + value.length);
    }

    @Override
    public void writeBytes(long index, byte[] buffer, int start, int length) {
        this.buffer.position((int) (index & Constants.INDEX_MASK_LOWER));
        this.buffer.put(buffer, start, length);
        store.onWriteUpTo(index + length);
    }

    @Override
    public void writeChar(long index, char value) {
        buffer.putChar((int) (index & Constants.INDEX_MASK_LOWER), value);
        store.onWriteUpTo(index + 2);
    }

    @Override
    public void writeShort(long index, short value) {
        buffer.putShort((int) (index & Constants.INDEX_MASK_LOWER), value);
        store.onWriteUpTo(index + 2);
    }

    @Override
    public void writeInt(long index, int value) {
        buffer.putInt((int) (index & Constants.INDEX_MASK_LOWER), value);
        store.onWriteUpTo(index + 4);
    }

    @Override
    public void writeLong(long index, long value) {
        buffer.putLong((int) (index & Constants.INDEX_MASK_LOWER), value);
        store.onWriteUpTo(index + 8);
    }

    @Override
    public void writeFloat(long index, float value) {
        buffer.putFloat((int) (index & Constants.INDEX_MASK_LOWER), value);
        store.onWriteUpTo(index + 4);
    }

    @Override
    public void writeDouble(long index, double value) {
        buffer.putDouble((int) (index & Constants.INDEX_MASK_LOWER), value);
        store.onWriteUpTo(index + 8);
    }
}
