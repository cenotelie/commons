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
     * The initial size of the edits buffers
     */
    private static final int EDITS_BUFFER_SIZE = 8;
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
     * The sequence number of the last transaction
     */
    private long endMark;
    /**
     * The starting indices of the stored edits
     */
    private int[] editIndex;
    /**
     * The content of the stored edits
     */
    private byte[][] editContent;
    /**
     * The number of edits
     */
    private int editCount;

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
        return editCount > 0;
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
     * Loads an edit made to this page
     *
     * @param offset  The offset of the edit within this page
     * @param content The edit's content
     */
    public void loadEdit(int offset, byte[] content) {
        System.arraycopy(content, 0, buffer, offset, content.length);
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
        state.set(STATE_READY);
    }

    /**
     * Compacts the edits contained in this page, if any
     */
    public void compact() {
        if (editCount <= 1)
            return;
        int[] fragmentBegin = new int[EDITS_BUFFER_SIZE];
        int[] fragmentLength = new int[EDITS_BUFFER_SIZE];
        int fragmentCount = 0;
        boolean compacted = false;

        for (int i = 0; i != editCount; i++) {
            int start = editIndex[i];
            int length = editContent[i].length;
            boolean handled = false;
            for (int j = 0; j != fragmentCount; j++) {
                if (start > fragmentBegin[j] + fragmentLength[j])
                    // this edit is strictly after the current fragment (not adjacent)
                    continue;
                if (start == fragmentBegin[j] + fragmentLength[j]) {
                    // this edit is right after the current fragment => extend this fragment
                    // TODO: continue here

                    compacted = true;
                    handled = true;
                    break;
                }
                if (fragmentBegin[j] > start + length) {
                    // this edit is completely before the current fragment, we must insert it here
                    if (fragmentCount == fragmentBegin.length) {
                        fragmentBegin = Arrays.copyOf(fragmentBegin, fragmentBegin.length * 2);
                        fragmentLength = Arrays.copyOf(fragmentLength, fragmentLength.length * 2);
                    }
                    // shift the fragments to the right
                    for (int k = fragmentCount - 1; k != j - 1; k--) {
                        fragmentBegin[k + 1] = fragmentBegin[k];
                        fragmentLength[k + 1] = fragmentLength[k];
                    }
                    // insert the edit as a new fragment here
                    fragmentBegin[j] = start;
                    fragmentLength[j] = length;
                    fragmentCount++;
                    handled = true;
                    break;
                }
                if (fragmentBegin[j] <= start + length) {
                    // this edit is just before (adjacent) to the current fragment, or the current fragment starts within the edit
                    // since we are here this edit is guaranteed to be strictly after the preceding fragment, if any
                    // so we can extend the current fragment to the left
                    fragmentBegin[j] = start;
                    // then check whether to extend this fragment to the right
                    int endFragment = fragmentBegin[j] + fragmentLength[j];
                    int endEdit = start + length;
                    if (endEdit > endFragment) {
                        // the edit ends after the modified fragment
                        // first, the new length of the fragment is the length of the edit
                        fragmentLength[j] = length;
                        // then, look for candidate fragments to be merged within this one
                        for (int k = j + 1; k != fragmentCount; k++) {
                            if (fragmentBegin[k] > fragmentBegin[j] + fragmentLength[j])
                                // strictly after (not adjacent)
                                break;
                            // merge the fragment k
                            // TODO: continue here
                        }
                    } else {
                        // the edit ends within, or just before the modified fragment
                        fragmentLength[j] = endFragment - start;
                    }
                    handled = true;
                    compacted = true;
                    break;
                }
            }
            if (!handled) {
                // add a new fragment at the end
                if (fragmentCount == fragmentBegin.length) {
                    fragmentBegin = Arrays.copyOf(fragmentBegin, fragmentBegin.length * 2);
                    fragmentLength = Arrays.copyOf(fragmentLength, fragmentLength.length * 2);
                }
                fragmentBegin[fragmentCount] = start;
                fragmentLength[fragmentCount] = length;
                fragmentCount++;
            }
        }

        if (!compacted)
            // we achieved nothing ...
            return;

        // initializes the new edits
        byte[][] newEditContent = new byte[fragmentCount][];
        for (int i = 0; i != fragmentCount; i++)
            newEditContent[i] = new byte[fragmentLength[i]];
        // apply the current edits to the new ones
        for (int i = 0; i != editCount; i++) {
            for (int j = 0; j != fragmentCount; j++) {
                if (editIndex[i] > fragmentBegin[j] + fragmentLength[j])
                    // this edit is strictly after the current fragment (not adjacent)
                    continue;
                // we are within the current fragment by construction
                System.arraycopy(
                        editContent[i],
                        0,
                        newEditContent[j],
                        editIndex[i] - fragmentBegin[j],
                        editContent[i].length
                );
                break;
            }
        }
        // commit
        editIndex = fragmentBegin;
        editContent = newEditContent;
        editCount = fragmentCount;
    }

    /**
     * Gets the log data for this page
     *
     * @return The log data
     */
    public LogTransactionPageData getLogData() {
        if (editCount == 0)
            return null;
        return new LogTransactionPageData(location, editIndex, editContent);
    }

    /**
     * Applies the edits of this page to the specified backend
     *
     * @param backend The backend
     */
    public void applyEditsTo(StorageBackend backend) {
        if (editCount == 0)
            return;
        for (int i = 0; i != editCount; i++) {
            try (StorageAccess access = backend.access(location + editIndex[i], editContent[i].length, true)) {
                access.writeBytes(editContent[i]);
            }
        }
    }

    /**
     * Releases this page
     */
    public void release() {
        state.set(STATE_FREE);
    }

    /**
     * Releases this page
     * At the end, whether there were edits or not, this page is at the state of the releasing transaction and is clean.
     *
     * @param sequenceNumber The sequence number of the releasing transaction
     */
    public void release(long sequenceNumber) {
        editIndex = null;
        editContent = null;
        editCount = 0;
        this.endMark = sequenceNumber;
        state.set(STATE_FREE);
    }

    /**
     * Registers a new edit made to this page
     *
     * @param index   The index of the edit in this page
     * @param content The edit's content
     */
    private void addEdit(int index, byte[] content) {
        if (editCount == 0) {
            editIndex = new int[EDITS_BUFFER_SIZE];
            editContent = new byte[EDITS_BUFFER_SIZE][];
        } else if (editCount >= editIndex.length) {
            editIndex = Arrays.copyOf(editIndex, editIndex.length * 2);
            editContent = Arrays.copyOf(editContent, editContent.length * 2);
        }
        editIndex[editCount] = index;
        editContent[editCount] = content;
        editCount++;
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
        addEdit(i, new byte[]{value});
    }

    @Override
    public void writeBytes(long index, byte[] value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        System.arraycopy(value, 0, buffer, i, value.length);
        addEdit(i, value);
    }

    @Override
    public void writeBytes(long index, byte[] buffer, int start, int length) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        System.arraycopy(buffer, start, this.buffer, i, length);
        if (start == 0 && buffer.length == length)
            addEdit(i, buffer);
        else
            addEdit(i, Arrays.copyOfRange(buffer, start, start + length));
    }

    @Override
    public void writeChar(long index, char value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        ByteUtils.setChar(buffer, i, value);
        addEdit(i, Arrays.copyOfRange(buffer, i, i + 2));
    }

    @Override
    public void writeShort(long index, short value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        ByteUtils.setShort(buffer, i, value);
        addEdit(i, Arrays.copyOfRange(buffer, i, i + 2));
    }

    @Override
    public void writeInt(long index, int value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        ByteUtils.setInt(buffer, i, value);
        addEdit(i, Arrays.copyOfRange(buffer, i, i + 4));
    }

    @Override
    public void writeLong(long index, long value) {
        int i = (int) (index & Constants.INDEX_MASK_LOWER);
        ByteUtils.setLong(buffer, i, value);
        addEdit(i, Arrays.copyOfRange(buffer, i, i + 8));
    }
}
