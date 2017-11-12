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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Represents a single file for storage with direct access to the file
 * This structure is thread-safe in the way it manages its inner data.
 * However, it does not ensure that multiple thread do not overlap while reading and writing to locations.
 *
 * @author Laurent Wouters
 */
public class RawFileDirect extends RawFile {
    /**
     * The endpoint for a direct file access
     */
    private final class Endpoint extends StorageEndpoint {
        /**
         * The access to the file
         */
        private final RandomAccessFile access;

        /**
         * Initializes this endpoint
         *
         * @param access The access to the file
         */
        public Endpoint(RandomAccessFile access) {
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
        public float readFloat(long index) {
            synchronized (access) {
                try {
                    access.seek(index);
                    return access.readFloat();
                } catch (IOException exception) {
                    throw new RuntimeException(exception);
                }
            }
        }

        @Override
        public double readDouble(long index) {
            synchronized (access) {
                try {
                    access.seek(index);
                    return access.readDouble();
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

        @Override
        public void writeFloat(long index, float value) {
            synchronized (access) {
                try {
                    access.seek(index);
                    access.writeFloat(value);
                } catch (IOException exception) {
                    throw new RuntimeException(exception);
                }
            }
        }

        @Override
        public void writeDouble(long index, double value) {
            synchronized (access) {
                try {
                    access.seek(index);
                    access.writeDouble(value);
                } catch (IOException exception) {
                    throw new RuntimeException(exception);
                }
            }
        }
    }


    /**
     * The accessed file
     */
    private final File file;
    /**
     * Whether the file is writable
     */
    private final boolean writable;
    /**
     * The access to the file
     */
    private final RandomAccessFile access;
    /**
     * The endpoint to use
     */
    private final Endpoint endpoint;

    /**
     * Initializes this data file
     *
     * @param file     The file location
     * @param writable Whether the file can be written to
     * @throws IOException When the file cannot be accessed
     */
    public RawFileDirect(File file, boolean writable) throws IOException {
        this.file = file;
        if (file.exists() && !file.canWrite())
            writable = false;
        this.writable = writable;
        this.access = new RandomAccessFile(file, writable ? "rw" : "r");
        this.endpoint = new Endpoint(access);
    }

    @Override
    public File getSystemFile() {
        return file;
    }

    @Override
    public boolean isWritable() {
        return writable;
    }

    @Override
    public long getSize() {
        try {
            return access.length();
        } catch (IOException exception) {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public boolean truncate(long length) throws IOException {
        if (access.length() <= length)
            return false;
        access.setLength(length);
        return true;
    }

    @Override
    public void flush() throws IOException {
        access.getChannel().force(true);
    }

    @Override
    public StorageEndpoint acquireEndpointAt(long index) {
        return endpoint;
    }

    @Override
    public void releaseEndpoint(StorageEndpoint endpoint) {
        // do nothing
    }

    @Override
    public void close() throws IOException {
        access.close();
    }
}
