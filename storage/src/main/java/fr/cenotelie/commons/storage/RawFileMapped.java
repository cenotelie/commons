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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Implements a raw file that is mapped to memory
 * This structure is not thread safe, unless its access is protected with an IOAccessManager
 *
 * @author Laurent Wouters
 */
public class RawFileMapped implements RawFile, IOEndpoint {
    /**
     * The accessed file
     */
    private final File file;
    /**
     * Whether the access is writable
     */
    private final boolean writable;
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
    public void flush() throws IOException {
        if (buffer != null)
            buffer.force();
    }

    @Override
    public IOEndpoint acquireEndpointAt(long index) {
        return this;
    }

    @Override
    public void releaseEndpoint(IOEndpoint endpoint) {
        // do nothing
    }

    @Override
    public void close() throws Exception {
        buffer = null;
    }

    @Override
    public byte readByte(int index) {
        return buffer.get(index);
    }

    @Override
    public byte[] readBytes(int index, int length) {
        byte[] result = new byte[length];
        readBytes(index, result, 0, length);
        return result;
    }

    @Override
    public synchronized void readBytes(int index, byte[] buffer, int start, int length) {
        this.buffer.position(index);
        this.buffer.get(buffer, start, length);
    }

    @Override
    public char readChar(int index) {
        return buffer.getChar(index);
    }

    @Override
    public int readInt(int index) {
        return buffer.getInt(index);
    }

    @Override
    public long readLong(int index) {
        return buffer.getLong(index);
    }

    @Override
    public float readFloat(int index) {
        return buffer.getFloat(index);
    }

    @Override
    public double readDouble(int index) {
        return buffer.getDouble(index);
    }

    @Override
    public void writeByte(int index, byte value) {
        buffer.put(index, value);
    }

    @Override
    public void writeBytes(int index, byte[] value) {
        writeBytes(index, value, 0, value.length);
    }

    @Override
    public void writeBytes(int index, byte[] buffer, int start, int length) {
        this.buffer.position(index);
        this.buffer.put(buffer, start, length);
    }

    @Override
    public void writeChar(int index, char value) {
        buffer.putChar(index, value);
    }

    @Override
    public void writeInt(int index, int value) {
        buffer.putInt(index, value);
    }

    @Override
    public void writeLong(int index, long value) {
        buffer.putLong(index, value);
    }

    @Override
    public void writeFloat(int index, float value) {
        buffer.putFloat(index, value);
    }

    @Override
    public void writeDouble(int index, double value) {
        buffer.putDouble(index, value);
    }
}
