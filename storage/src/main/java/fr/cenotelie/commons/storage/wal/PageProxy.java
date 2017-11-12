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
 * Represents a WAL page that is really a proxy onto the page held by the backend storage system
 *
 * @author Laurent Wouters
 */
class PageProxy extends Page {
    /**
     * The endpoint used to access the backend storage system
     */
    private final StorageEndpoint endpoint;

    /**
     * Initializes this page
     *
     * @param location The position of the page within the backing system
     * @param endpoint The endpoint used to access the backend storage system
     */
    public PageProxy(long location, StorageEndpoint endpoint) {
        super(location);
        this.endpoint = endpoint;
    }

    @Override
    public long getIndexLowerBound() {
        return 0;
    }

    @Override
    public long getIndexUpperBound() {
        return 0;
    }

    @Override
    public byte readByte(long index) {
        return endpoint.readByte(index);
    }

    @Override
    public byte[] readBytes(long index, int length) {
        return endpoint.readBytes(index, length);
    }

    @Override
    public void readBytes(long index, byte[] buffer, int start, int length) {
        endpoint.readBytes(index, buffer, start, length);
    }

    @Override
    public char readChar(long index) {
        return endpoint.readChar(index);
    }

    @Override
    public short readShort(long index) {
        return 0;
    }

    @Override
    public int readInt(long index) {
        return endpoint.readInt(index);
    }

    @Override
    public long readLong(long index) {
        return endpoint.readLong(index);
    }

    @Override
    public float readFloat(long index) {
        return endpoint.readFloat(index);
    }

    @Override
    public double readDouble(long index) {
        return endpoint.readDouble(index);
    }

    @Override
    public void writeShort(long index, short value) {

    }
}
