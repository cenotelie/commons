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

package fr.cenotelie.commons.storage;

import fr.cenotelie.commons.utils.ByteUtils;

/**
 * Represents a controlled access to storage system.
 * The access defines a span that can be accessed within the storage system.
 * Only operations within this span are allowed.
 * The beginning of the span is a mapped to the 0 index of this access element.
 * The access element keeps track of its current index within the span and will automatically update it upon reading and writing.
 *
 * @author Laurent Wouters
 */
public class Access implements AutoCloseable {
    /**
     * The target storage system for this access
     */
    private Storage storage;
    /**
     * The current endpoint
     */
    private Endpoint endpoint;
    /**
     * The lower bound for indices within the scope of this endpoint
     */
    private long endpointBoundMin;
    /**
     * The upper bound (excluded) for indices within the scope of this endpoint
     */
    private long endpointBoundMax;

    /**
     * The location in the storage system
     */
    private long location;
    /**
     * The length of the proxy in the storage system
     */
    private int length;
    /**
     * Whether the access allows writing
     */
    private boolean writable;
    /**
     * The current index in the storage system
     */
    private long index;

    /**
     * Initializes an empty access
     */
    public Access() {
        this.storage = null;
        this.endpoint = null;
        this.location = -1;
        this.length = 0;
        this.writable = false;
        this.index = -1;
    }

    /**
     * Initializes this access
     *
     * @param storage  The target storage system for this access
     * @param location The location of the span for this access within the storage system
     * @param length   The length of the allowed span
     * @param writable Whether the access allows writing
     */
    public Access(Storage storage, long location, int length, boolean writable) {
        this.storage = storage;
        this.endpoint = null;
        this.endpointBoundMin = Long.MAX_VALUE;
        this.endpointBoundMax = Long.MIN_VALUE;
        this.location = location;
        this.length = length;
        this.writable = writable;
        this.index = location;
    }

    /**
     * Setups this access before using it
     *
     * @param storage  The target storage system for this access
     * @param location The location of the span for this access within the storage system
     * @param length   The length of the allowed span
     * @param writable Whether the access allows writing
     */
    protected void setup(Storage storage, long location, int length, boolean writable) {
        this.storage = storage;
        this.endpoint = null;
        this.endpointBoundMin = Long.MAX_VALUE;
        this.endpointBoundMax = Long.MIN_VALUE;
        this.location = location;
        this.length = length;
        this.writable = writable;
        this.index = location;
    }

    /**
     * Gets the location of this access in the storage system
     *
     * @return The location of this access in the storage system
     */
    public long getLocation() {
        return location;
    }

    /**
     * Gets the current index of this access
     * The index is local to this access, meaning that 0 represents the start of the access window in the associated storage system.
     *
     * @return The current access index
     */
    public long getIndex() {
        return (index - location);
    }

    /**
     * Gets the length of this access window in the associated storage system
     *
     * @return The length of this access window
     */
    public int getLength() {
        return length;
    }

    /**
     * Gets whether this access is disjoint from the specified one
     *
     * @param access An access
     * @return Whether the two access are disjoint
     */
    public boolean disjoints(Access access) {
        return (this.location + this.length <= access.location) // this is completely before parameter
                || (access.location + access.length <= this.location); // parameter is completely before this
    }

    /**
     * Gets whether the access allows writing
     *
     * @return Whether the access allows writing
     */
    public boolean isWritable() {
        return writable;
    }

    /**
     * Positions the index of this access
     * The index is local to this access, meaning that 0 represents the start of the access window in the associated storage system.
     *
     * @param index The new access index
     * @return This access
     */
    public Access seek(int index) {
        this.index = location + index;
        return this;
    }

    /**
     * Resets the index of this access to its initial position
     * The index is local to this access, meaning that 0 represents the start of the access window in the associated storage system.
     *
     * @return This access
     */
    public Access reset() {
        this.index = location;
        return this;
    }

    /**
     * Moves the index of this access
     * The index is local to this access, meaning that 0 represents the start of the access window in the associated storage system.
     *
     * @param offset The offset to move from
     * @return This access
     */
    public Access skip(int offset) {
        this.index += offset;
        return this;
    }

    /**
     * Gets whether the specified number of bytes at the current index are within the allowed bounds
     *
     * @param length A number of bytes
     * @return true if the bytes are within the bounds
     */
    public boolean isWithinAccessBounds(int length) {
        return (index >= location && index + length <= location + this.length);
    }

    /**
     * Updates the endpoint for the current index
     */
    private void updateEndpoint() {
        try {
            if (endpoint != null)
                storage.releaseEndpoint(endpoint);
            endpoint = storage.acquireEndpointAt(index);
        } catch (Throwable throwable) {
            endpoint = null;
            endpointBoundMin = Long.MAX_VALUE;
            endpointBoundMax = Long.MIN_VALUE;
            throw throwable;
        }
        endpointBoundMin = endpoint.getIndexLowerBound();
        endpointBoundMax = endpoint.getIndexUpperBound();
    }

    /**
     * Reads a single byte at the current index
     *
     * @return The byte
     */
    public byte readByte() {
        if (!isWithinAccessBounds(1))
            throw new IndexOutOfBoundsException("Cannot read the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        byte value = endpoint.readByte(index);
        index++;
        return value;
    }

    /**
     * Reads a specified number of bytes a the current index
     *
     * @param length The number of bytes to read
     * @return The bytes
     */
    public byte[] readBytes(int length) {
        if (!isWithinAccessBounds(length))
            throw new IndexOutOfBoundsException("Cannot read the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + length <= endpointBoundMax) {
            // fast track for access within the current endpoint
            byte[] value = endpoint.readBytes(index, length);
            index += length;
            return value;
        }
        byte[] buffer = new byte[length];
        int remaining = length;
        int target = 0;
        while (remaining > 0) {
            if (index + remaining <= endpointBoundMax) {
                endpoint.readBytes(index, buffer, target, remaining);
                index += remaining;
                break;
            } else {
                int toRead = (int) (endpointBoundMax - index);
                endpoint.readBytes(index, buffer, target, toRead);
                remaining -= toRead;
                target += toRead;
                index += toRead;
                updateEndpoint();
            }
        }
        return buffer;
    }

    /**
     * Reads a specified number of bytes a the current index
     *
     * @param buffer The buffer to fill
     * @param start  The index in the buffer to start filling at
     * @param length The number of bytes to read
     */
    public void readBytes(byte[] buffer, int start, int length) {
        if (!isWithinAccessBounds(length))
            throw new IndexOutOfBoundsException("Cannot read the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + length <= endpointBoundMax) {
            // fast track for access within the current endpoint
            endpoint.readBytes(index, buffer, start, length);
            index += length;
            return;
        }
        int remaining = length;
        int target = start;
        while (remaining > 0) {
            if (index + remaining <= endpointBoundMax) {
                endpoint.readBytes(index, buffer, target, remaining);
                index += remaining;
                break;
            } else {
                int toRead = (int) (endpointBoundMax - index);
                endpoint.readBytes(index, buffer, target, toRead);
                remaining -= toRead;
                target += toRead;
                index += toRead;
                updateEndpoint();
            }
        }
    }

    /**
     * Reads a single char at the current index
     *
     * @return The char
     */
    public char readChar() {
        if (!isWithinAccessBounds(2))
            throw new IndexOutOfBoundsException("Cannot read the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + 2 <= endpointBoundMax) {
            // fast track for access within the current endpoint
            char value = endpoint.readChar(index);
            index += 2;
            return value;
        }
        byte b0 = endpoint.readByte(index);
        index++;
        updateEndpoint();
        byte b1 = endpoint.readByte(index);
        index++;
        return ByteUtils.getChar(b0, b1);
    }

    /**
     * Reads a single short at the current index
     *
     * @return The short
     */
    public short readShort() {
        if (!isWithinAccessBounds(2))
            throw new IndexOutOfBoundsException("Cannot read the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + 2 <= endpointBoundMax) {
            // fast track for access within the current endpoint
            short value = endpoint.readShort(index);
            index += 2;
            return value;
        }
        byte b0 = endpoint.readByte(index);
        index++;
        updateEndpoint();
        byte b1 = endpoint.readByte(index);
        index++;
        return ByteUtils.getShort(b0, b1);
    }

    /**
     * Reads a single int at the current index
     *
     * @return The int
     */
    public int readInt() {
        if (!isWithinAccessBounds(4))
            throw new IndexOutOfBoundsException("Cannot read the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + 4 <= endpointBoundMax) {
            // fast track for access within the current endpoint
            int value = endpoint.readInt(index);
            index += 4;
            return value;
        }
        byte b0 = endpoint.readByte(index);
        index++;
        if (index >= endpointBoundMax)
            updateEndpoint();
        byte b1 = endpoint.readByte(index);
        index++;
        if (index >= endpointBoundMax)
            updateEndpoint();
        byte b2 = endpoint.readByte(index);
        index++;
        if (index >= endpointBoundMax)
            updateEndpoint();
        byte b3 = endpoint.readByte(index);
        index++;
        return ByteUtils.getInt(b0, b1, b2, b3);
    }

    /**
     * Reads a single long at the current index
     *
     * @return The long
     */
    public long readLong() {
        if (!isWithinAccessBounds(8))
            throw new IndexOutOfBoundsException("Cannot read the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + 8 <= endpointBoundMax) {
            // fast track for access within the current endpoint
            long value = endpoint.readLong(index);
            index += 8;
            return value;
        }
        byte b0 = endpoint.readByte(index);
        index++;
        if (index >= endpointBoundMax)
            updateEndpoint();
        byte b1 = endpoint.readByte(index);
        index++;
        if (index >= endpointBoundMax)
            updateEndpoint();
        byte b2 = endpoint.readByte(index);
        index++;
        if (index >= endpointBoundMax)
            updateEndpoint();
        byte b3 = endpoint.readByte(index);
        index++;
        if (index >= endpointBoundMax)
            updateEndpoint();
        byte b4 = endpoint.readByte(index);
        index++;
        if (index >= endpointBoundMax)
            updateEndpoint();
        byte b5 = endpoint.readByte(index);
        index++;
        if (index >= endpointBoundMax)
            updateEndpoint();
        byte b6 = endpoint.readByte(index);
        index++;
        if (index >= endpointBoundMax)
            updateEndpoint();
        byte b7 = endpoint.readByte(index);
        index++;
        return ByteUtils.getLong(b0, b1, b2, b3, b4, b5, b6, b7);
    }

    /**
     * Reads a single float at the current index
     *
     * @return The float
     */
    public float readFloat() {
        return Float.floatToIntBits(readInt());
    }

    /**
     * Reads a single double at the current index
     *
     * @return The double
     */
    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    /**
     * Writes a single byte at the current index
     *
     * @param value The byte to write
     * @return This access
     */
    public Access writeByte(byte value) {
        if (!writable || !isWithinAccessBounds(1))
            throw new IndexOutOfBoundsException("Cannot write the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        endpoint.writeByte(index, value);
        index++;
        return this;
    }

    /**
     * Writes bytes at the current index
     *
     * @param value The bytes to write
     * @return This access
     */
    public Access writeBytes(byte[] value) {
        if (!writable || !isWithinAccessBounds(value.length))
            throw new IndexOutOfBoundsException("Cannot write the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + length <= endpointBoundMax) {
            // fast track for access within the current endpoint
            endpoint.writeBytes(index, value);
            index += value.length;
            return this;
        }
        int remaining = length;
        int target = 0;
        while (remaining > 0) {
            if (index + remaining <= endpointBoundMax) {
                endpoint.writeBytes(index, value, target, remaining);
                index += remaining;
                break;
            } else {
                int toWrite = (int) (endpointBoundMax - index);
                endpoint.writeBytes(index, value, target, toWrite);
                remaining -= toWrite;
                target += toWrite;
                index += toWrite;
                updateEndpoint();
            }
        }
        return this;
    }

    /**
     * Writes bytes at the current index
     *
     * @param buffer The buffer with the bytes to write
     * @param start  The index in the buffer to start writing from
     * @param length The number of bytes to write
     * @return This access
     */
    public Access writeBytes(byte[] buffer, int start, int length) {
        if (!writable || !isWithinAccessBounds(length))
            throw new IndexOutOfBoundsException("Cannot write the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + length <= endpointBoundMax) {
            // fast track for access within the current endpoint
            endpoint.writeBytes(index, buffer, start, length);
            index += length;
            return this;
        }
        int remaining = length;
        int target = start;
        while (remaining > 0) {
            if (index + remaining <= endpointBoundMax) {
                endpoint.writeBytes(index, buffer, target, remaining);
                index += remaining;
                break;
            } else {
                int toWrite = (int) (endpointBoundMax - index);
                endpoint.writeBytes(index, buffer, target, toWrite);
                remaining -= toWrite;
                target += toWrite;
                index += toWrite;
                updateEndpoint();
            }
        }
        return this;
    }

    /**
     * Writes a single char at the current index
     *
     * @param value The char to write
     * @return This access
     */
    public Access writeChar(char value) {
        if (!writable || !isWithinAccessBounds(2))
            throw new IndexOutOfBoundsException("Cannot write the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + 2 <= endpointBoundMax) {
            // fast track for access within the current endpoint
            endpoint.writeChar(index, value);
            index += 2;
            return this;
        }
        endpoint.writeByte(index, (byte) (value >>> 8 & 0xFF));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value & 0xFF));
        index++;
        return this;
    }

    /**
     * Writes a single short at the current index
     *
     * @param value The short to write
     * @return This access
     */
    public Access writeShort(short value) {
        if (!writable || !isWithinAccessBounds(2))
            throw new IndexOutOfBoundsException("Cannot write the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + 2 <= endpointBoundMax) {
            // fast track for access within the current endpoint
            endpoint.writeShort(index, value);
            index += 2;
            return this;
        }
        endpoint.writeByte(index, (byte) (value >>> 8 & 0xFF));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value & 0xFF));
        index++;
        return this;
    }

    /**
     * Writes a single int at the current index
     *
     * @param value The int to write
     * @return This access
     */
    public Access writeInt(int value) {
        if (!writable || !isWithinAccessBounds(4))
            throw new IndexOutOfBoundsException("Cannot write the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + 4 <= endpointBoundMax) {
            // fast track for access within the current endpoint
            endpoint.writeInt(index, value);
            index += 4;
            return this;
        }
        endpoint.writeByte(index, (byte) (value >>> 24 & 0xFF));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value >>> 16 & 0xFF));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value >>> 8 & 0xFF));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value & 0xFF));
        index++;
        return this;
    }

    /**
     * Writes a single long at the current index
     *
     * @param value The long to write
     * @return This access
     */
    public Access writeLong(long value) {
        if (!writable || !isWithinAccessBounds(4))
            throw new IndexOutOfBoundsException("Cannot write the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + 8 <= endpointBoundMax) {
            // fast track for access within the current endpoint
            endpoint.writeLong(index, value);
            index += 8;
            return this;
        }
        endpoint.writeByte(index, (byte) (value >>> 56 & 0xFF));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value >>> 48 & 0xFF));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value >>> 40 & 0xFF));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value >>> 32 & 0xFF));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value >>> 24 & 0xFF));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value >>> 16 & 0xFF));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value >>> 8 & 0xFF));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value & 0xFF));
        index++;
        return this;
    }

    /**
     * Writes a single float at the current index
     *
     * @param value The float to write
     * @return This access
     */
    public Access writeFloat(float value) {
        return writeInt(Float.floatToIntBits(value));
    }

    /**
     * Writes a single double at the current index
     *
     * @param value The double to write
     * @return This access
     */
    public Access writeDouble(double value) {
        return writeLong(Double.doubleToLongBits(value));
    }

    @Override
    public void close() {
        releaseOnClose();
    }

    /**
     * Releases any held resource
     */
    protected void releaseOnClose() {
        if (storage != null && endpoint != null) {
            try {
                storage.releaseEndpoint(endpoint);
            } finally {
                storage = null;
                endpoint = null;
                endpointBoundMin = Long.MAX_VALUE;
                endpointBoundMax = Long.MIN_VALUE;
            }
        } else {
            storage = null;
            endpoint = null;
        }
    }

    @Override
    public String toString() {
        return (writable ? "W" : "R") + "[0x" + Long.toHexString(location) + ", 0x" + Long.toHexString(location + length) + ")";
    }
}
