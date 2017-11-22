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
    private final RawFileDirectEndpoint endpoint;

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
        this.endpoint = new RawFileDirectEndpoint(access);
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
    public boolean cut(long from, long to) throws IOException {
        if (from < 0 || from > to)
            throw new IndexOutOfBoundsException();
        if (from == to)
            // 0-length cut => do nothing
            return false;
        long currentSize = access.length();
        if (from >= currentSize)
            // start after the current size => no effect
            return false;
        if (to >= currentSize) {
            // truncate the file
            access.setLength(from);
        } else {
            // overwrite the cut content with 0
            access.seek(from);
            for (long i = from; i != to; i++) {
                access.writeByte(0);
            }
        }
        return true;
    }

    @Override
    public void flush() throws IOException {
        access.getChannel().force(true);
    }

    @Override
    public Endpoint acquireEndpointAt(long index) {
        return endpoint;
    }

    @Override
    public void releaseEndpoint(Endpoint endpoint) {
        // do nothing
    }

    @Override
    public void close() throws IOException {
        access.close();
    }
}
