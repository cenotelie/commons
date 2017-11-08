/*******************************************************************************
 * Copyright (c) 2016 Association Cénotélie (cenotelie.fr)
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
 * Represents an endpoint for IO operations, i.e. a structure that supports data being read from and written to
 *
 * @author Laurent Wouters
 */
public interface IOEndpoint {
    /**
     * Reads a single byte at the current index
     *
     * @param index The index within this element for this operation
     * @return The byte
     */
    byte readByte(long index);

    /**
     * Reads a specified number of bytes a the current index
     *
     * @param index  The index within this element for this operation
     * @param length The number of bytes to read
     * @return The bytes
     */
    byte[] readBytes(long index, int length);

    /**
     * Reads a specified number of bytes a the current index
     *
     * @param index  The index within this element for this operation
     * @param buffer The buffer to fill
     * @param start  The index in the buffer to start filling at
     * @param length The number of bytes to read
     */
    void readBytes(long index, byte[] buffer, int start, int length);

    /**
     * Reads a single char at the current index
     *
     * @param index The index within this element for this operation
     * @return The char
     */
    char readChar(long index);

    /**
     * Reads a single int at the current index
     *
     * @param index The index within this element for this operation
     * @return The int
     */
    int readInt(long index);

    /**
     * Reads a single long at the current index
     *
     * @param index The index within this element for this operation
     * @return The long
     */
    long readLong(long index);

    /**
     * Reads a single float at the current index
     *
     * @param index The index within this element for this operation
     * @return The float
     */
    float readFloat(long index);

    /**
     * Reads a single double at the current index
     *
     * @param index The index within this element for this operation
     * @return The double
     */
    double readDouble(long index);

    /**
     * Writes a single byte at the current index
     *
     * @param index The index within this element for this operation
     * @param value The byte to write
     */
    void writeByte(long index, byte value);

    /**
     * Writes bytes at the current index
     *
     * @param index The index within this element for this operation
     * @param value The bytes to write
     */
    void writeBytes(long index, byte[] value);

    /**
     * Writes bytes at the current index
     *
     * @param index  The index within this element for this operation
     * @param buffer The buffer with the bytes to write
     * @param start  The index in the buffer to start writing from
     * @param length The number of bytes to write
     */
    void writeBytes(long index, byte[] buffer, int start, int length);

    /**
     * Writes a single char at the current index
     *
     * @param index The index within this element for this operation
     * @param value The char to write
     */
    void writeChar(long index, char value);

    /**
     * Writes a single int at the current index
     *
     * @param index The index within this element for this operation
     * @param value The int to write
     */
    void writeInt(long index, int value);

    /**
     * Writes a single long at the current index
     *
     * @param index The index within this element for this operation
     * @param value The long to write
     */
    void writeLong(long index, long value);

    /**
     * Writes a single float at the current index
     *
     * @param index The index within this element for this operation
     * @param value The float to write
     */
    void writeFloat(long index, float value);

    /**
     * Writes a single double at the current index
     *
     * @param index The index within this element for this operation
     * @param value The double to write
     */
    void writeDouble(long index, double value);
}
