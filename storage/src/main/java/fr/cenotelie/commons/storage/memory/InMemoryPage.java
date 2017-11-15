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

import fr.cenotelie.commons.storage.ByteUtils;
import fr.cenotelie.commons.storage.Constants;
import fr.cenotelie.commons.storage.StorageEndpoint;

import java.util.Arrays;

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
    private final byte[] buffer;

    /**
     * Initializes this page
     *
     * @param store    The parent store
     * @param location The location of the page
     */
    public InMemoryPage(InMemoryStore store, long location) {
        this.store = store;
        this.location = location;
        this.buffer = new byte[Constants.PAGE_SIZE];
    }

    /**
     * Gets the location of the page within the backing system
     *
     * @return The location of the page within the backing system
     */
    public long getLocation() {
        return location;
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
        store.onWriteUpTo(index + 1);
    }

    @Override
    public void writeBytes(long index, byte[] value) {
        writeBytes(index, value, 0, value.length);
    }

    @Override
    public void writeBytes(long index, byte[] buffer, int start, int length) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        System.arraycopy(buffer, start, this.buffer, i, length);
        store.onWriteUpTo(index + length);
    }

    @Override
    public void writeChar(long index, char value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        ByteUtils.setChar(buffer, i, value);
        store.onWriteUpTo(index + 2);
    }

    @Override
    public void writeShort(long index, short value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        ByteUtils.setShort(buffer, i, value);
        store.onWriteUpTo(index + 2);
    }

    @Override
    public void writeInt(long index, int value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        ByteUtils.setInt(buffer, i, value);
        store.onWriteUpTo(index + 4);
    }

    @Override
    public void writeLong(long index, long value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        ByteUtils.setLong(buffer, i, value);
        store.onWriteUpTo(index + 8);
    }
}
