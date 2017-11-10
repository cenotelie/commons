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

import fr.cenotelie.commons.storage.StorageEndpoint;

/**
 * Represents a page of data protected by a write-ahead log as seen by a transaction
 *
 * @author Laurent Wouters
 */
abstract class Page extends StorageEndpoint {
    /**
     * The number of bits to use in order to represent an index within a page
     */
    public static final int PAGE_INDEX_LENGTH = 13;
    /**
     * The size of a page in bytes
     */
    public static final int PAGE_SIZE = 1 << PAGE_INDEX_LENGTH;
    /**
     * The mask for the index within a page
     */
    public static final long INDEX_MASK_LOWER = PAGE_SIZE - 1;

    /**
     * The location of the page within the backing system
     */
    protected final long location;

    /**
     * Gets the location of the page within the backing system
     *
     * @return The location of the page within the backing system
     */
    public long getLocation() {
        return location;
    }

    /**
     * Gets whether this page can be written to
     *
     * @return Whether this page can be written to
     */
    public boolean isWritable() {
        return false;
    }

    /**
     * Gets whether this page has been touched by the current transaction
     *
     * @return Whether this page has been touched by the current transaction
     */
    public boolean isDirty() {
        return false;
    }

    /**
     * Initializes this page
     *
     * @param location The position of the page within the backing system
     */
    public Page(long location) {
        this.location = location;
    }

    @Override
    public void writeByte(long index, byte value) {
        throw new Error("This page is read-only");
    }

    @Override
    public void writeBytes(long index, byte[] value) {
        throw new Error("This page is read-only");
    }

    @Override
    public void writeBytes(long index, byte[] buffer, int start, int length) {
        throw new Error("This page is read-only");
    }

    @Override
    public void writeChar(long index, char value) {
        throw new Error("This page is read-only");
    }

    @Override
    public void writeInt(long index, int value) {
        throw new Error("This page is read-only");
    }

    @Override
    public void writeLong(long index, long value) {
        throw new Error("This page is read-only");
    }

    @Override
    public void writeFloat(long index, float value) {
        throw new Error("This page is read-only");
    }

    @Override
    public void writeDouble(long index, double value) {
        throw new Error("This page is read-only");
    }
}
