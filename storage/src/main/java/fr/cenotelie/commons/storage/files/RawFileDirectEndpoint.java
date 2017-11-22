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

import fr.cenotelie.commons.storage.Endpoint;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * The endpoint for a direct file access
 *
 * @author Laurent Wouters
 */
class RawFileDirectEndpoint extends Endpoint {
    /**
     * The access to the file
     */
    private final RandomAccessFile access;

    /**
     * Initializes this endpoint
     *
     * @param access The access to the file
     */
    public RawFileDirectEndpoint(RandomAccessFile access) {
        this.access = access;
    }

    @Override
    public long getIndexLowerBound() {
        return 0;
    }

    @Override
    public long getIndexUpperBound() {
        return Long.MAX_VALUE - 1;
    }

    @Override
    public byte readByte(long index) {
        synchronized (access) {
            try {
                access.seek(index);
                return access.readByte();
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }
    }

    @Override
    public byte[] readBytes(long index, int length) {
        byte[] buffer = new byte[length];
        readBytes(index, buffer, 0, length);
        return buffer;
    }

    @Override
    public void readBytes(long index, byte[] buffer, int start, int length) {
        synchronized (access) {
            try {
                access.seek(index);
                access.read(buffer, start, length);
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }
    }

    @Override
    public char readChar(long index) {
        synchronized (access) {
            try {
                access.seek(index);
                return access.readChar();
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }
    }

    @Override
    public short readShort(long index) {
        synchronized (access) {
            try {
                access.seek(index);
                return access.readShort();
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }
    }

    @Override
    public int readInt(long index) {
        synchronized (access) {
            try {
                access.seek(index);
                return access.readInt();
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }
    }

    @Override
    public long readLong(long index) {
        synchronized (access) {
            try {
                access.seek(index);
                return access.readLong();
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }
    }

    @Override
    public void writeByte(long index, byte value) {
        synchronized (access) {
            try {
                access.seek(index);
                access.writeByte(value);
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }
    }

    @Override
    public void writeBytes(long index, byte[] value) {
        writeBytes(index, value, 0, value.length);
    }

    @Override
    public void writeBytes(long index, byte[] buffer, int start, int length) {
        synchronized (access) {
            try {
                access.seek(index);
                access.write(buffer, start, length);
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }
    }

    @Override
    public void writeChar(long index, char value) {
        synchronized (access) {
            try {
                access.seek(index);
                access.writeChar(value);
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }
    }

    @Override
    public void writeShort(long index, short value) {
        synchronized (access) {
            try {
                access.seek(index);
                access.writeShort(value);
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }
    }

    @Override
    public void writeInt(long index, int value) {
        synchronized (access) {
            try {
                access.seek(index);
                access.writeInt(value);
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }
    }

    @Override
    public void writeLong(long index, long value) {
        synchronized (access) {
            try {
                access.seek(index);
                access.writeLong(value);
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }
    }
}
