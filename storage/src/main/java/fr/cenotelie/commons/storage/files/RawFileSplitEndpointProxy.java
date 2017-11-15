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

import fr.cenotelie.commons.storage.StorageEndpoint;

/**
 * A proxy endpoint for a split file
 *
 * @author Laurent Wouters
 */
class RawFileSplitEndpointProxy extends StorageEndpoint {
    /**
     * The file that provided the original endpoint
     */
    private final RawFile file;
    /**
     * The original endpoint
     */
    private final StorageEndpoint endpoint;
    /**
     * The offset of this endpoint relative to the original one
     */
    private final long offset;
    /**
     * The maximum size of a part file
     */
    private final long maxSize;

    /**
     * Initializes this endpoint
     *
     * @param file     The file that provided the original endpoint
     * @param endpoint The original endpoint
     * @param offset   The offset of this endpoint relative to the original one
     * @param maxSize  The maximum size of a part file
     */
    public RawFileSplitEndpointProxy(RawFile file, StorageEndpoint endpoint, long offset, long maxSize) {
        this.file = file;
        this.endpoint = endpoint;
        this.offset = offset;
        this.maxSize = maxSize;
    }

    /**
     * Releases this endpoint
     */
    public void release() {
        file.releaseEndpoint(endpoint);
    }

    @Override
    public long getIndexLowerBound() {
        return endpoint.getIndexLowerBound() + offset;
    }

    @Override
    public long getIndexUpperBound() {
        long bound = endpoint.getIndexUpperBound();
        if (bound > maxSize)
            bound = maxSize;
        return bound + offset;
    }

    @Override
    public byte readByte(long index) {
        return endpoint.readByte(index - offset);
    }

    @Override
    public byte[] readBytes(long index, int length) {
        return endpoint.readBytes(index - offset, length);
    }

    @Override
    public void readBytes(long index, byte[] buffer, int start, int length) {
        endpoint.readBytes(index - offset, buffer, start, length);
    }

    @Override
    public char readChar(long index) {
        return endpoint.readChar(index - offset);
    }

    @Override
    public short readShort(long index) {
        return endpoint.readShort(index - offset);
    }

    @Override
    public int readInt(long index) {
        return endpoint.readInt(index - offset);
    }

    @Override
    public long readLong(long index) {
        return endpoint.readLong(index - offset);
    }

    @Override
    public void writeByte(long index, byte value) {
        endpoint.writeByte(index - offset, value);
    }

    @Override
    public void writeBytes(long index, byte[] value) {
        endpoint.writeBytes(index - offset, value);
    }

    @Override
    public void writeBytes(long index, byte[] buffer, int start, int length) {
        endpoint.writeBytes(index - offset, buffer, start, length);
    }

    @Override
    public void writeChar(long index, char value) {
        endpoint.writeChar(index - offset, value);
    }

    @Override
    public void writeShort(long index, short value) {
        endpoint.writeShort(index - offset, value);
    }

    @Override
    public void writeInt(long index, int value) {
        endpoint.writeInt(index - offset, value);
    }

    @Override
    public void writeLong(long index, long value) {
        endpoint.writeLong(index - offset, value);
    }
}
