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

package fr.cenotelie.commons.storage.raw;

import fr.cenotelie.commons.storage.StorageEndpoint;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Implements a raw file that is mapped to memory
 * This structure is not thread safe, unless its access is protected with an TSAccessManager
 *
 * @author Laurent Wouters
 */
public class RawFileMapped extends RawFile {
    /**
     * The IO endpoint for this file
     */
    private final class Endpoint extends StorageEndpoint {
        /**
         * The memory-mapped byte buffer for a file
         */
        private final MappedByteBuffer buffer;

        /**
         * Initializes this endpoint
         *
         * @param buffer The
         */
        public Endpoint(MappedByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public byte readByte(long index) {
            return buffer.get((int) index);
        }

        @Override
        public byte[] readBytes(long index, int length) {
            byte[] result = new byte[length];
            readBytes(index, result, 0, length);
            return result;
        }

        @Override
        public synchronized void readBytes(long index, byte[] buffer, int start, int length) {
            this.buffer.position((int) index);
            this.buffer.get(buffer, start, length);
        }

        @Override
        public char readChar(long index) {
            return buffer.getChar((int) index);
        }

        @Override
        public int readInt(long index) {
            return buffer.getInt((int) index);
        }

        @Override
        public long readLong(long index) {
            return buffer.getLong((int) index);
        }

        @Override
        public float readFloat(long index) {
            return buffer.getFloat((int) index);
        }

        @Override
        public double readDouble(long index) {
            return buffer.getDouble((int) index);
        }

        @Override
        public void writeByte(long index, byte value) {
            buffer.put((int) index, value);
        }

        @Override
        public void writeBytes(long index, byte[] value) {
            writeBytes(index, value, 0, value.length);
        }

        @Override
        public void writeBytes(long index, byte[] buffer, int start, int length) {
            this.buffer.position((int) index);
            this.buffer.put(buffer, start, length);
        }

        @Override
        public void writeChar(long index, char value) {
            buffer.putChar((int) index, value);
        }

        @Override
        public void writeInt(long index, int value) {
            buffer.putInt((int) index, value);
        }

        @Override
        public void writeLong(long index, long value) {
            buffer.putLong((int) index, value);
        }

        @Override
        public void writeFloat(long index, float value) {
            buffer.putFloat((int) index, value);
        }

        @Override
        public void writeDouble(long index, double value) {
            buffer.putDouble((int) index, value);
        }
    }

    /**
     * The accessed file
     */
    private final File file;
    /**
     * Whether the access is writable
     */
    private final boolean writable;
    /**
     * The endpoint for this file
     */
    private Endpoint endpoint;
    /**
     * The memory-mapped byte buffer for a file
     */
    private MappedByteBuffer buffer;

    /**
     * Initializes this file
     *
     * @param file     The accessed file
     * @param writable Whether the file can be written to
     * @throws IOException When the file cannot be accessed
     */
    public RawFileMapped(File file, boolean writable) throws IOException {
        this.file = file;
        this.writable = writable && (!file.exists() || file.canWrite());
        try (FileChannel channel = new RandomAccessFile(file, this.writable ? "rw" : "r").getChannel()) {
            this.buffer = channel.map(this.writable ? FileChannel.MapMode.READ_WRITE : FileChannel.MapMode.READ_ONLY, 0, channel.size());
        }
        this.endpoint = new Endpoint(this.buffer);
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
        return buffer.capacity();
    }

    @Override
    public void flush() throws IOException {
        if (buffer != null)
            buffer.force();
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
    public void close() {
        buffer = null;
        endpoint = null;
    }
}
