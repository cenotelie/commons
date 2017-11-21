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
import fr.cenotelie.commons.utils.ByteUtils;

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
    private byte[] buffer;
    /**
     * The location of the page within the backing system
     */
    private long location;
    /**
     * The edits for this page
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
            buffer = new byte[Constants.PAGE_SIZE];
        int length = Constants.PAGE_SIZE;
        if (location + Constants.PAGE_SIZE > backend.getSize())
            length = (int) (backend.getSize() - location);
        if (length > 0) {
            try (StorageAccess access = backend.access(location, length, false)) {
                access.readBytes(buffer, 0, length);
            }
        }
        if (length < Constants.PAGE_SIZE)
            Arrays.fill(buffer, length, Constants.PAGE_SIZE - length, (byte) 0);
    }

    /**
     * Loads the edits made by a previous transaction to this page
     *
     * @param access The access to use for loading
     * @param data   The data of the transaction
     */
    public void loadEdits(StorageAccess access, LogPageData data) {
        access.skip(8 + 4); // skip the location data and number of edits
        for (int i = 0; i != data.editsCount; i++) {
            int offset = PageEdits.editIndex(data.edits[i]);
            int length = PageEdits.editLength(data.edits[i]);
            access.skip(8); // skip the offset and length
            access.readBytes(buffer, offset, length);
        }
    }

    /**
     * Once reserved, setups this page so that it is ready
     *
     * @param location The location of the page within the backing system
     */
    public void makeReady(long location) {
        this.location = location;
        state.set(STATE_READY);
    }

    /**
     * Gets the log data for this page
     *
     * @return The log data
     */
    public LogPageData getLogData(int offset) {
        if (edits == null)
            return null;
        return new LogPageData(offset, location, edits, buffer);
    }

    /**
     * Releases this page
     * At the end, whether there were edits or not, this page is at the state of the releasing transaction and is clean.
     */
    public void release() {
        edits = null;
        state.set(STATE_FREE);
    }

    /**
     * Registers a new edit made to this page
     *
     * @param index  The index of the edit in this page
     * @param length The length of the edit
     */
    private void addEdit(int index, int length) {
        if (edits == null)
            edits = new PageEdits();
        edits.addEdit(index, length);
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
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        buffer[i] = value;
        addEdit(i, 1);
    }

    @Override
    public void writeBytes(long index, byte[] value) {
        writeBytes(index, value, 0, value.length);
    }

    @Override
    public void writeBytes(long index, byte[] buffer, int start, int length) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        System.arraycopy(buffer, start, this.buffer, i, length);
        addEdit(i, length);
    }

    @Override
    public void writeChar(long index, char value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        ByteUtils.setChar(buffer, i, value);
        addEdit(i, 2);
    }

    @Override
    public void writeShort(long index, short value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        ByteUtils.setShort(buffer, i, value);
        addEdit(i, 2);
    }

    @Override
    public void writeInt(long index, int value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        ByteUtils.setInt(buffer, i, value);
        addEdit(i, 4);
    }

    @Override
    public void writeLong(long index, long value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        ByteUtils.setLong(buffer, i, value);
        addEdit(i, 8);
    }
}
