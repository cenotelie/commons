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

/**
 * Base API for a controlled access to a storage backend element
 * The access defines a span within the backend element that can be accessed.
 * Only operations within this span are allowed.
 * The beginning of the span is a mapped to the 0 index of this access element.
 * The access element keeps track of its current index within the span and will automatically update it upon reading and writing.
 *
 * @author Laurent Wouters
 */
public class StorageAccess implements AutoCloseable {
    /**
     * The target backend for this access
     */
    private StorageBackend backend;
    /**
     * The current endpoint
     */
    private StorageEndpoint endpoint;
    /**
     * The lower bound for indices within the scope of this endpoint
     */
    private long endpointBoundMin;
    /**
     * The upper bound (excluded) for indices within the scope of this endpoint
     */
    private long endpointBoundMax;

    /**
     * The location in the backend
     */
    private long location;
    /**
     * The length of the proxy in the backend
     */
    private int length;
    /**
     * Whether the access allows writing
     */
    private boolean writable;
    /**
     * The current index in the backend
     */
    private long index;

    /**
     * Initializes an empty access
     */
    public StorageAccess() {
        this.backend = null;
        this.endpoint = null;
        this.location = -1;
        this.length = 0;
        this.writable = false;
        this.index = -1;
    }

    /**
     * Initializes this access
     *
     * @param backend  The target backend for this access
     * @param location The location of the span for this access within the backend
     * @param length   The length of the allowed span
     * @param writable Whether the access allows writing
     */
    public StorageAccess(StorageBackend backend, long location, int length, boolean writable) {
        this.backend = backend;
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
     * @param backend  The target backend for this access
     * @param location The location of the span for this access within the backend
     * @param length   The length of the allowed span
     * @param writable Whether the access allows writing
     */
    protected void setup(StorageBackend backend, long location, int length, boolean writable) {
        this.backend = backend;
        this.endpoint = null;
        this.endpointBoundMin = Long.MAX_VALUE;
        this.endpointBoundMax = Long.MIN_VALUE;
        this.location = location;
        this.length = length;
        this.writable = writable;
        this.index = location;
    }

    /**
     * Gets the location of this access in the backend
     *
     * @return The location of this access in the backend
     */
    public long getLocation() {
        return location;
    }

    /**
     * Gets the current index of this access
     * The index is local to this access, meaning that 0 represents the start of the access window in the associated backend.
     *
     * @return The current access index
     */
    public long getIndex() {
        return (index - location);
    }

    /**
     * Gets the length of this access window in the associated backend
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
    public boolean disjoints(StorageAccess access) {
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
     * The index is local to this access, meaning that 0 represents the start of the access window in the associated backend.
     *
     * @param index The new access index
     * @return This access
     */
    public StorageAccess seek(int index) {
        this.index = location + index;
        return this;
    }

    /**
     * Resets the index of this access to its initial position
     * The index is local to this access, meaning that 0 represents the start of the access window in the associated backend.
     *
     * @return This access
     */
    public StorageAccess reset() {
        this.index = location;
        return this;
    }

    /**
     * Moves the index of this access
     * The index is local to this access, meaning that 0 represents the start of the access window in the associated backend.
     *
     * @param offset The offset to move from
     * @return This access
     */
    public StorageAccess skip(int offset) {
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
                backend.releaseEndpoint(endpoint);
            endpoint = backend.acquireEndpointAt(index);
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
        return (char) (b0 << 8 | b1 & 0xFF);
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
        return (short) (b0 << 8 | b1 & 0xFF);
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
        return b0 << 24
                | (b1 & 255) << 16
                | (b2 & 255) << 8
                | b3 & 255;
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
        return (long) b0 << 56
                | ((long) b1 & 255L) << 48
                | ((long) b2 & 255L) << 40
                | ((long) b3 & 255L) << 32
                | ((long) b4 & 255L) << 24
                | ((long) b5 & 255L) << 16
                | ((long) b6 & 255L) << 8
                | (long) b7 & 255L;
    }

    /**
     * Reads a single float at the current index
     *
     * @return The float
     */
    public float readFloat() {
        if (!isWithinAccessBounds(4))
            throw new IndexOutOfBoundsException("Cannot read the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + 4 <= endpointBoundMax) {
            // fast track for access within the current endpoint
            float value = endpoint.readFloat(index);
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
        return Float.intBitsToFloat(b0 << 24
                | (b1 & 255) << 16
                | (b2 & 255) << 8
                | b3 & 255);
    }

    /**
     * Reads a single double at the current index
     *
     * @return The double
     */
    public double readDouble() {
        if (!isWithinAccessBounds(8))
            throw new IndexOutOfBoundsException("Cannot read the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + 8 <= endpointBoundMax) {
            // fast track for access within the current endpoint
            double value = endpoint.readDouble(index);
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
        return Double.longBitsToDouble((long) b0 << 56
                | ((long) b1 & 255L) << 48
                | ((long) b2 & 255L) << 40
                | ((long) b3 & 255L) << 32
                | ((long) b4 & 255L) << 24
                | ((long) b5 & 255L) << 16
                | ((long) b6 & 255L) << 8
                | (long) b7 & 255L);
    }

    /**
     * Writes a single byte at the current index
     *
     * @param value The byte to write
     */
    public void writeByte(byte value) {
        if (!writable || !isWithinAccessBounds(1))
            throw new IndexOutOfBoundsException("Cannot write the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        endpoint.writeByte(index, value);
        index++;
    }

    /**
     * Writes bytes at the current index
     *
     * @param value The bytes to write
     */
    public void writeBytes(byte[] value) {
        if (!writable || !isWithinAccessBounds(value.length))
            throw new IndexOutOfBoundsException("Cannot write the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + length <= endpointBoundMax) {
            // fast track for access within the current endpoint
            endpoint.writeBytes(index, value);
            index += value.length;
            return;
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
    }

    /**
     * Writes bytes at the current index
     *
     * @param buffer The buffer with the bytes to write
     * @param start  The index in the buffer to start writing from
     * @param length The number of bytes to write
     */
    public void writeBytes(byte[] buffer, int start, int length) {
        if (!writable || !isWithinAccessBounds(length))
            throw new IndexOutOfBoundsException("Cannot write the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + length <= endpointBoundMax) {
            // fast track for access within the current endpoint
            endpoint.writeBytes(index, buffer, start, length);
            index += length;
            return;
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
    }

    /**
     * Writes a single char at the current index
     *
     * @param value The char to write
     */
    public void writeChar(char value) {
        if (!writable || !isWithinAccessBounds(2))
            throw new IndexOutOfBoundsException("Cannot write the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + 2 <= endpointBoundMax) {
            // fast track for access within the current endpoint
            endpoint.writeChar(index, value);
            index += 2;
            return;
        }
        endpoint.writeByte(index, (byte) (value >> 8));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) value);
        index++;
    }

    /**
     * Writes a single short at the current index
     *
     * @param value The short to write
     */
    public void writeShort(short value) {
        if (!writable || !isWithinAccessBounds(2))
            throw new IndexOutOfBoundsException("Cannot write the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + 2 <= endpointBoundMax) {
            // fast track for access within the current endpoint
            endpoint.writeShort(index, value);
            index += 2;
            return;
        }
        endpoint.writeByte(index, (byte) (value >> 8));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) value);
        index++;
    }

    /**
     * Writes a single int at the current index
     *
     * @param value The int to write
     */
    public void writeInt(int value) {
        if (!writable || !isWithinAccessBounds(4))
            throw new IndexOutOfBoundsException("Cannot write the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + 4 <= endpointBoundMax) {
            // fast track for access within the current endpoint
            endpoint.writeInt(index, value);
            index += 4;
            return;
        }
        endpoint.writeByte(index, (byte) (value >> 24));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value >> 16));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value >> 8));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) value);
        index++;
    }

    /**
     * Writes a single long at the current index
     *
     * @param value The long to write
     */
    public void writeLong(long value) {
        if (!writable || !isWithinAccessBounds(4))
            throw new IndexOutOfBoundsException("Cannot write the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + 8 <= endpointBoundMax) {
            // fast track for access within the current endpoint
            endpoint.writeLong(index, value);
            index += 8;
            return;
        }
        endpoint.writeByte(index, (byte) (value >> 56));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value >> 48));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value >> 40));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value >> 32));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value >> 24));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value >> 16));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (value >> 8));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) value);
        index++;
    }

    /**
     * Writes a single float at the current index
     *
     * @param value The float to write
     */
    public void writeFloat(float value) {
        if (!writable || !isWithinAccessBounds(4))
            throw new IndexOutOfBoundsException("Cannot write the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + 4 <= endpointBoundMax) {
            // fast track for access within the current endpoint
            endpoint.writeFloat(index, value);
            index += 4;
            return;
        }
        int valueInt = Float.floatToIntBits(value);
        endpoint.writeByte(index, (byte) (valueInt >> 24));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (valueInt >> 16));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (valueInt >> 8));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) valueInt);
        index++;
    }

    /**
     * Writes a single double at the current index
     *
     * @param value The double to write
     */
    public void writeDouble(double value) {
        if (!writable || !isWithinAccessBounds(8))
            throw new IndexOutOfBoundsException("Cannot write the specified amount of data at this index");
        if (index < endpointBoundMin || index >= endpointBoundMax)
            updateEndpoint();
        if (index + 8 <= endpointBoundMax) {
            // fast track for access within the current endpoint
            endpoint.writeDouble(index, value);
            index += 8;
            return;
        }
        long valueLong = Double.doubleToLongBits(value);
        endpoint.writeByte(index, (byte) (valueLong >> 56));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (valueLong >> 48));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (valueLong >> 40));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (valueLong >> 32));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (valueLong >> 24));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (valueLong >> 16));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) (valueLong >> 8));
        index++;
        updateEndpoint();
        endpoint.writeByte(index, (byte) valueLong);
        index++;
    }

    @Override
    public void close() {
        releaseOnClose();
    }

    /**
     * Releases any held resource
     */
    protected void releaseOnClose() {
        if (backend != null && endpoint != null) {
            try {
                backend.releaseEndpoint(endpoint);
            } finally {
                backend = null;
                endpoint = null;
                endpointBoundMin = Long.MAX_VALUE;
                endpointBoundMax = Long.MIN_VALUE;
            }
        } else {
            backend = null;
            endpoint = null;
        }
    }

    @Override
    public String toString() {
        return (writable ? "W" : "R") + "[0x" + Long.toHexString(location) + ", 0x" + Long.toHexString(location + length) + ")";
    }
}
