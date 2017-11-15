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
 * Utility class for byte manipulation
 *
 * @author Laurent Wouters
 */
public class ByteUtils {
    /**
     * Gets a char value from a buffer
     *
     * @param buffer The buffer
     * @param i      The index in the buffer
     * @return The value
     */
    public static char getChar(byte[] buffer, int i) {
        return getChar(buffer[i], buffer[i + 1]);
    }

    /**
     * Gets a char value
     *
     * @param b0 Byte 0
     * @param b1 Byte 1
     * @return The value
     */
    public static char getChar(byte b0, byte b1) {
        return (char) (((b0 & 0xFF) << 8)
                | (b1 & 0xFF));
    }

    /**
     * Gets a short value from a buffer
     *
     * @param buffer The buffer
     * @param i      The index in the buffer
     * @return The value
     */
    public static short getShort(byte[] buffer, int i) {
        return getShort(buffer[i], buffer[i + 1]);
    }

    /**
     * Gets a short value
     *
     * @param b0 Byte 0
     * @param b1 Byte 1
     * @return The value
     */
    public static short getShort(byte b0, byte b1) {
        return (short) (((b0 & 0xFF) << 8)
                | (b1 & 0xFF));
    }

    /**
     * Gets an integer value from a buffer
     *
     * @param buffer The buffer
     * @param i      The index in the buffer
     * @return The value
     */
    public static int getInt(byte[] buffer, int i) {
        return getInt(buffer[i], buffer[i + 1], buffer[i + 2], buffer[i + 3]);
    }

    /**
     * Gets an integer value
     *
     * @param b0 Byte 0
     * @param b1 Byte 1
     * @param b2 Byte 2
     * @param b3 Byte 3
     * @return The value
     */
    public static int getInt(byte b0, byte b1, byte b2, byte b3) {
        return (((b0 & 0xFF) << 24)
                | ((b1 & 0xFF) << 16)
                | ((b2 & 0xFF) << 8)
                | (b3 & 0xFF));
    }

    /**
     * Gets a long value from a buffer
     *
     * @param buffer The buffer
     * @param i      The index in the buffer
     * @return The value
     */
    public static long getLong(byte[] buffer, int i) {
        return getLong(buffer[i], buffer[i + 1], buffer[i + 2], buffer[i + 3], buffer[i + 4], buffer[i + 5], buffer[i + 6], buffer[i + 7]);
    }

    /**
     * Gets a long value
     *
     * @param b0 Byte 0
     * @param b1 Byte 1
     * @param b2 Byte 2
     * @param b3 Byte 3
     * @param b4 Byte 4
     * @param b5 Byte 5
     * @param b6 Byte 6
     * @param b7 Byte 7
     * @return The value
     */
    public static long getLong(byte b0, byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7) {
        return (((long) b0 & 0xFF) << 56
                | ((long) b1 & 0xFF) << 48
                | ((long) b2 & 0xFF) << 40
                | ((long) b3 & 0xFF) << 32
                | ((long) b4 & 0xFF) << 24
                | ((long) b5 & 0xFF) << 16
                | ((long) b6 & 0xFF) << 8
                | ((long) b7 & 0xFF));
    }

    /**
     * Sets a char value in a buffer
     *
     * @param buffer The buffer
     * @param i      the index in the buffer
     * @param value  The value
     */
    public static void setChar(byte[] buffer, int i, char value) {
        buffer[i] = (byte) (value >>> 8 & 0xFF);
        buffer[i + 1] = (byte) (value & 0xFF);
    }

    /**
     * Sets a short value in a buffer
     *
     * @param buffer The buffer
     * @param i      the index in the buffer
     * @param value  The value
     */
    public static void setShort(byte[] buffer, int i, short value) {
        buffer[i] = (byte) (value >>> 8 & 0xFF);
        buffer[i + 1] = (byte) (value & 0xFF);
    }

    /**
     * Sets an integer value in a buffer
     *
     * @param buffer The buffer
     * @param i      the index in the buffer
     * @param value  The value
     */
    public static void setInt(byte[] buffer, int i, int value) {
        buffer[i] = (byte) (value >>> 24 & 0xFF);
        buffer[i + 1] = (byte) (value >>> 16 & 0xFF);
        buffer[i + 2] = (byte) (value >>> 8 & 0xFF);
        buffer[i + 3] = (byte) (value & 0xFF);
    }

    /**
     * Sets a long value in a buffer
     *
     * @param buffer The buffer
     * @param i      the index in the buffer
     * @param value  The value
     */
    public static void setLong(byte[] buffer, int i, long value) {
        buffer[i] = (byte) (value >>> 56 & 0xFF);
        buffer[i + 1] = (byte) (value >>> 48 & 0xFF);
        buffer[i + 2] = (byte) (value >>> 40 & 0xFF);
        buffer[i + 3] = (byte) (value >>> 32 & 0xFF);
        buffer[i + 4] = (byte) (value >>> 24 & 0xFF);
        buffer[i + 5] = (byte) (value >>> 16 & 0xFF);
        buffer[i + 6] = (byte) (value >>> 8 & 0xFF);
        buffer[i + 7] = (byte) (value & 0xFF);
    }
}
