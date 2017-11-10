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
import fr.cenotelie.commons.storage.StorageEndpoint;
import fr.cenotelie.commons.storage.files.RawFile;

import java.nio.ByteBuffer;

/**
 * Represents a page that is buffered in-memory, most likely because it has been modified within the WAL
 *
 * @author Laurent Wouters
 */
class PageBuffered extends Page {
    /**
     * The page's current content as seen by the transaction using this page
     */
    protected final ByteBuffer buffer;

    /**
     * Initializes this page
     *
     * @param location The position of the page within the backing system
     */
    public PageBuffered(long location) {
        super(location);
        this.buffer = ByteBuffer.allocate(Constants.PAGE_SIZE);
    }

    /**
     * Reads the initial content of this page from the backend storage system
     *
     * @param backend The backend storage system
     */
    public void load(RawFile backend) {
        try (StorageEndpoint endpoint = backend.acquireEndpointAt(location)) {
            endpoint.readBytes(location, buffer.array(), 0, Constants.PAGE_SIZE);
        }
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
}
