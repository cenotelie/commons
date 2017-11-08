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
    byte readByte(int index);

    /**
     * Reads a specified number of bytes a the current index
     *
     * @param index  The index within this element for this operation
     * @param length The number of bytes to read
     * @return The bytes
     */
    byte[] readBytes(int index, int length);

    /**
     * Reads a specified number of bytes a the current index
     *
     * @param index  The index within this element for this operation
     * @param buffer The buffer to fill
     * @param start  The index in the buffer to start filling at
     * @param length The number of bytes to read
     */
    void readBytes(int index, byte[] buffer, int start, int length);

    /**
     * Reads a single char at the current index
     *
     * @param index The index within this element for this operation
     * @return The char
     */
    char readChar(int index);

    /**
     * Reads a single int at the current index
     *
     * @param index The index within this element for this operation
     * @return The int
     */
    int readInt(int index);

    /**
     * Reads a single long at the current index
     *
     * @param index The index within this element for this operation
     * @return The long
     */
    long readLong(int index);

    /**
     * Reads a single float at the current index
     *
     * @param index The index within this element for this operation
     * @return The float
     */
    float readFloat(int index);

    /**
     * Reads a single double at the current index
     *
     * @param index The index within this element for this operation
     * @return The double
     */
    double readDouble(int index);

    /**
     * Writes a single byte at the current index
     *
     * @param index The index within this element for this operation
     * @param value The byte to write
     */
    void writeByte(int index, byte value);

    /**
     * Writes bytes at the current index
     *
     * @param index The index within this element for this operation
     * @param value The bytes to write
     */
    void writeBytes(int index, byte[] value);

    /**
     * Writes bytes at the current index
     *
     * @param index  The index within this element for this operation
     * @param buffer The buffer with the bytes to write
     * @param start  The index in the buffer to start writing from
     * @param length The number of bytes to write
     */
    void writeBytes(int index, byte[] buffer, int start, int length);

    /**
     * Writes a single char at the current index
     *
     * @param index The index within this element for this operation
     * @param value The char to write
     */
    void writeChar(int index, char value);

    /**
     * Writes a single int at the current index
     *
     * @param index The index within this element for this operation
     * @param value The int to write
     */
    void writeInt(int index, int value);

    /**
     * Writes a single long at the current index
     *
     * @param index The index within this element for this operation
     * @param value The long to write
     */
    void writeLong(int index, long value);

    /**
     * Writes a single float at the current index
     *
     * @param index The index within this element for this operation
     * @param value The float to write
     */
    void writeFloat(int index, float value);

    /**
     * Writes a single double at the current index
     *
     * @param index The index within this element for this operation
     * @param value The double to write
     */
    void writeDouble(int index, double value);
}
