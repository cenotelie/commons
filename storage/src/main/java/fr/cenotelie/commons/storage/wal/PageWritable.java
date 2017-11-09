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

import java.util.Arrays;

/**
 * Implements a writable page
 *
 * @author Laurent Wouters
 */
class PageWritable extends PageBuffered {
    /**
     * The current edits for this page
     */
    protected final PageEdits edits;

    /**
     * Initializes this page
     *
     * @param position The position of the page within the backing system
     */
    public PageWritable(long position) {
        super(position);
        this.edits = new PageEdits();
    }

    @Override
    public boolean isWritable() {
        return true;
    }

    public boolean isDirty() {
        return edits.getEditCount() > 0;
    }

    @Override
    public void writeByte(long index, byte value) {
        int shortIndex = (int) (index & INDEX_MASK_LOWER);
        buffer.put(shortIndex, value);
        edits.push(shortIndex, new byte[]{value});
    }

    @Override
    public void writeBytes(long index, byte[] value) {
        writeBytes(index, value, 0, value.length);
    }

    @Override
    public void writeBytes(long index, byte[] buffer, int start, int length) {
        int shortIndex = (int) (index & INDEX_MASK_LOWER);
        this.buffer.position(shortIndex);
        this.buffer.put(buffer, start, length);
        edits.push(shortIndex, Arrays.copyOfRange(buffer, start, length));
    }

    @Override
    public void writeChar(long index, char value) {
        int shortIndex = (int) (index & INDEX_MASK_LOWER);
        buffer.putChar(shortIndex, value);
        buffer.position(shortIndex);
        byte[] content = new byte[2];
        buffer.get(content);
        edits.push(shortIndex, content);
    }

    @Override
    public void writeInt(long index, int value) {
        int shortIndex = (int) (index & INDEX_MASK_LOWER);
        buffer.putInt(shortIndex, value);
        buffer.position(shortIndex);
        byte[] content = new byte[4];
        buffer.get(content);
        edits.push(shortIndex, content);
    }

    @Override
    public void writeLong(long index, long value) {
        int shortIndex = (int) (index & INDEX_MASK_LOWER);
        buffer.putLong(shortIndex, value);
        buffer.position(shortIndex);
        byte[] content = new byte[8];
        buffer.get(content);
        edits.push(shortIndex, content);
    }

    @Override
    public void writeFloat(long index, float value) {
        int shortIndex = (int) (index & INDEX_MASK_LOWER);
        buffer.putFloat(shortIndex, value);
        buffer.position(shortIndex);
        byte[] content = new byte[4];
        buffer.get(content);
        edits.push(shortIndex, content);
    }

    @Override
    public void writeDouble(long index, double value) {
        int shortIndex = (int) (index & INDEX_MASK_LOWER);
        buffer.putDouble(shortIndex, value);
        buffer.position(shortIndex);
        byte[] content = new byte[8];
        buffer.get(content);
        edits.push(shortIndex, content);
    }
}
