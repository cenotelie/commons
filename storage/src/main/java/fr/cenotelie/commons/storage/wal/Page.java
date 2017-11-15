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

package fr.cenotelie.commons.storage.wal;

import fr.cenotelie.commons.storage.Constants;
import fr.cenotelie.commons.storage.StorageAccess;
import fr.cenotelie.commons.storage.StorageBackend;
import fr.cenotelie.commons.storage.StorageEndpoint;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a page of data protected by a write-ahead log as seen by a transaction
 *
 * @author Laurent Wouters
 */
class Page extends StorageEndpoint {
    /**
     * The page is free, i.e. not assigned to any location
     */
    private static final int STATE_FREE = 0;
    /**
     * The page is reserved, i.e. is going to contain data but is not ready yet
     */
    private static final int STATE_RESERVED = 1;
    /**
     * The page exists and is ready for IO
     */
    private static final int STATE_READY = 3;

    /**
     * The state of this page
     */
    private final AtomicInteger state;
    /**
     * The page's current content as seen by the transaction using this page
     */
    private ByteBuffer buffer;
    /**
     * The location of the page within the backing system
     */
    private long location;
    /**
     * The sequence number of the last transaction
     */
    private long endMark;
    /**
     * The edits, if any
     */
    private PageEdits edits;

    /**
     * Initializes this page
     */
    public Page() {
        this.state = new AtomicInteger(STATE_FREE);
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
     * Gets the sequence number of the last transaction
     *
     * @return The sequence number of the last transaction
     */
    public long getEndMark() {
        return endMark;
    }

    /**
     * Gets whether this page has been touched by the current transaction
     *
     * @return Whether this page has been touched by the current transaction
     */
    public boolean isDirty() {
        return edits != null;
    }

    /**
     * Tries to reserve this page
     *
     * @return Whether the reservation was successful
     */
    public boolean reserve() {
        return state.compareAndSet(STATE_FREE, STATE_RESERVED);
    }

    /**
     * Loads the base content of this page from the backend
     *
     * @param backend  The backend to load from
     * @param location The location in the backend to load from
     */
    public void loadBase(StorageBackend backend, long location) {
        if (buffer == null)
            buffer = ByteBuffer.allocate(Constants.PAGE_SIZE);
        int length = Constants.PAGE_SIZE;
        if (location + Constants.PAGE_SIZE > backend.getSize())
            length = (int) (backend.getSize() - location);
        try (StorageAccess access = backend.access(location, length, false)) {
            access.readBytes(buffer.array(), 0, length);
        }
        if (length < Constants.PAGE_SIZE)
            Arrays.fill(buffer.array(), length, Constants.PAGE_SIZE - length, (byte) 0);
    }

    /**
     * Loads an edit made to this page
     *
     * @param offset  The offset of the edit within this page
     * @param content The edit's content
     */
    public void loadEdit(int offset, byte[] content) {
        buffer.position(offset);
        buffer.put(content);
    }

    /**
     * Once reserved, setups this page so that it is ready
     *
     * @param location The location of the page within the backing system
     * @param endMark  The sequence number of the last transaction
     */
    public void makeReady(long location, long endMark) {
        this.location = location;
        this.endMark = endMark;
        buffer.position(0);
        state.set(STATE_READY);
    }

    /**
     * Releases this page
     */
    public void release() {
        state.set(STATE_FREE);
    }

    /**
     * Registers a new edit made to this page
     *
     * @param index   The index of the edit in this page
     * @param content The edit's content
     */
    private void addEdit(int index, byte[] content) {
        if (edits == null)
            edits = new PageEdits();
        edits.push(index, content);
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
        int shortIndex = (int) (index & Constants.INDEX_MASK_LOWER);
        buffer.put(shortIndex, value);
        addEdit(shortIndex, new byte[]{value});
    }

    @Override
    public void writeBytes(long index, byte[] value) {
        writeBytes(index, value, 0, value.length);
    }

    @Override
    public void writeBytes(long index, byte[] buffer, int start, int length) {
        int shortIndex = (int) (index & Constants.INDEX_MASK_LOWER);
        this.buffer.position(shortIndex);
        this.buffer.put(buffer, start, length);
        addEdit(shortIndex, Arrays.copyOfRange(buffer, start, length));
    }

    @Override
    public void writeChar(long index, char value) {
        int shortIndex = (int) (index & Constants.INDEX_MASK_LOWER);
        buffer.putChar(shortIndex, value);
        buffer.position(shortIndex);
        byte[] content = new byte[2];
        buffer.get(content);
        addEdit(shortIndex, content);
    }

    @Override
    public void writeShort(long index, short value) {
        int shortIndex = (int) (index & Constants.INDEX_MASK_LOWER);
        buffer.putShort(shortIndex, value);
        buffer.position(shortIndex);
        byte[] content = new byte[2];
        buffer.get(content);
        addEdit(shortIndex, content);
    }

    @Override
    public void writeInt(long index, int value) {
        int shortIndex = (int) (index & Constants.INDEX_MASK_LOWER);
        buffer.putInt(shortIndex, value);
        buffer.position(shortIndex);
        byte[] content = new byte[4];
        buffer.get(content);
        addEdit(shortIndex, content);
    }

    @Override
    public void writeLong(long index, long value) {
        int shortIndex = (int) (index & Constants.INDEX_MASK_LOWER);
        buffer.putLong(shortIndex, value);
        buffer.position(shortIndex);
        byte[] content = new byte[8];
        buffer.get(content);
        addEdit(shortIndex, content);
    }

    @Override
    public void writeFloat(long index, float value) {
        int shortIndex = (int) (index & Constants.INDEX_MASK_LOWER);
        buffer.putFloat(shortIndex, value);
        buffer.position(shortIndex);
        byte[] content = new byte[4];
        buffer.get(content);
        addEdit(shortIndex, content);
    }

    @Override
    public void writeDouble(long index, double value) {
        int shortIndex = (int) (index & Constants.INDEX_MASK_LOWER);
        buffer.putDouble(shortIndex, value);
        buffer.position(shortIndex);
        byte[] content = new byte[8];
        buffer.get(content);
        addEdit(shortIndex, content);
    }
}
