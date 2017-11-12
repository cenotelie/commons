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
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a file storage that spans multiple files.
 * This structure is thread-safe in the way it manages its inner data.
 * However, it does not ensure that multiple thread do not overlap while reading and writing to locations.
 *
 * @author Laurent Wouters
 */
public class RawFileSplit extends RawFile {
    /**
     * The maximum number of part files
     */
    public static final int MAX_FILES = 9999;

    /**
     * The backend is ready for IO
     */
    private static final int STATE_READY = 0;
    /**
     * The backend is currently busy with an operation
     */
    private static final int STATE_BUSY = 1;
    /**
     * The backend is now closed
     */
    private static final int STATE_CLOSED = -1;

    /**
     * The directory that contains the files
     */
    private final File directory;
    /**
     * The prefix for part files
     */
    private final String filePrefix;
    /**
     * The suffix for part files
     */
    private final String fileSuffix;
    /**
     * The factory to use to instantiate raw file accesses
     */
    private final RawFileFactory factory;
    /**
     * Whether the files are writable
     */
    private final boolean writable;
    /**
     * The maximum size of part files
     */
    private final long fileMaxSize;
    /**
     * The number of part files
     */
    private final AtomicInteger filesCount;
    /**
     * The part files
     */
    private volatile RawFile[] files;
    /**
     * The current state of this storage
     */
    private final AtomicInteger state;

    /**
     * Initializes this storage
     *
     * @param directory The directory that contains the files
     * @param prefix    The prefix for part files
     * @param suffix    The suffix for part files
     * @param factory   The factory to use to instantiate raw file accesses
     * @param writable  Whether the files are writable
     * @param maxSize   The maximum size of part files
     */
    public RawFileSplit(File directory, String prefix, String suffix, RawFileFactory factory, boolean writable, long maxSize) {
        this.directory = directory;
        this.filePrefix = prefix;
        this.fileSuffix = suffix;
        this.factory = factory;
        this.writable = writable;
        this.fileMaxSize = maxSize;
        this.filesCount = new AtomicInteger(0);
        for (int i = 0; i != MAX_FILES; i++) {
            File file = new File(directory, fileName(i));
            if (!file.exists()) {
                filesCount.set(i);
                files = new RawFile[bufferSize(i)];
                break;
            }
        }
        this.state = new AtomicInteger(STATE_READY);
    }

    /**
     * Gets the size of the buffer (power of 2) that can hold the specified count
     *
     * @param count A number of files
     * @return The size of the buffer
     */
    private int bufferSize(int count) {
        int size = 2; // minimum buffer size
        while (size < count) {
            size = size << 1;
        }
        return size;
    }

    /**
     * Gets the file name for the n-th fie
     *
     * @param index The index of the file
     * @return The name of the file
     */
    private String fileName(int index) {
        if (index < 10)
            return filePrefix + "000" + index + fileSuffix;
        if (index < 100)
            return filePrefix + "00" + index + fileSuffix;
        if (index < 1000)
            return filePrefix + "0" + index + fileSuffix;
        return filePrefix + index + fileSuffix;
    }

    @Override
    public File getSystemFile() {
        return directory;
    }

    @Override
    public boolean isWritable() {
        return writable;
    }

    @Override
    public long getSize() {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                return 0;
            if (state.compareAndSet(STATE_READY, STATE_BUSY))
                break;
        }
        try {
            int count = filesCount.get();
            if (count == 0)
                return 0;
            long total = (count - 1) * fileMaxSize;
            if (files[count - 1] == null)
                files[count - 1] = factory.newStorage(new File(directory, fileName(count - 1)), writable);
            total += files[count - 1].getSize();
            return total;
        } catch (IOException exception) {
            throw new RuntimeException(exception);
        } finally {
            state.set(STATE_READY);
        }
    }

    @Override
    public boolean truncate(long length) throws IOException {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new IOException("The file is closed");
            if (state.compareAndSet(STATE_READY, STATE_BUSY))
                break;
        }
        try {
            return doTruncate(length);
        } finally {
            state.set(STATE_READY);
        }
    }

    /**
     * Truncates this storage system to the specified length
     *
     * @param length The length to truncate to
     * @return Whether the operation had an effect
     * @throws IOException When an IO error occurred
     */
    private boolean doTruncate(long length) throws IOException {
        int fileIndex = (int) (length / fileMaxSize);
        long rest = length % fileMaxSize;
        int count = filesCount.get();
        if (fileIndex >= count)
            return false;
        for (int i = fileIndex + 1; i != count; i++) {
            // close the file
            if (files[i] != null)
                files[i].close();
        }
        for (int i = fileIndex + 1; i != count; i++) {
            // delete the file
            File target = new File(directory, fileName(i));
            if (target.exists() && !target.delete())
                throw new IOException("Failed to delete file " + target.getAbsolutePath());
        }
        // truncate the last file
        if (rest == 0) {
            // delete
            if (files[fileIndex] != null)
                files[fileIndex].close();
            File target = new File(directory, fileName(fileIndex));
            filesCount.set(fileIndex);
            if (target.exists() && !target.delete())
                throw new IOException("Failed to delete file " + target.getAbsolutePath());
            return true;
        } else {
            // truncate to rest
            filesCount.set(fileIndex + 1);
            if (files[fileIndex] == null)
                files[fileIndex] = factory.newStorage(new File(directory, fileName(fileIndex)), writable);
            boolean deletedSomeFiles = fileIndex + 1 != count;
            return deletedSomeFiles || files[fileIndex].truncate(rest);
        }
    }

    @Override
    public void flush() throws IOException {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new IOException("The file is closed");
            if (state.compareAndSet(STATE_READY, STATE_BUSY))
                break;
        }
        try {
            for (int i = 0; i != filesCount.get(); i++) {
                if (files[i] != null)
                    files[i].flush();
            }
        } finally {
            state.set(STATE_READY);
        }
    }

    @Override
    public StorageEndpoint acquireEndpointAt(long index) {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new RuntimeException(new IOException("The file is closed"));
            if (state.compareAndSet(STATE_READY, STATE_BUSY))
                break;
        }
        try {
            return doAcquireEndpointAt(index);
        } catch (IOException exception) {
            throw new RuntimeException(exception);
        } finally {
            state.set(STATE_READY);
        }
    }

    /**
     * Acquires an endpoint that enables reading and writing to the backend at the specified index
     * The endpoint must be subsequently released by a call to
     *
     * @param index An index within this backend
     * @return The corresponding endpoint
     */
    private StorageEndpoint doAcquireEndpointAt(long index) throws IOException {
        int fileIndex = (int) (index / fileMaxSize);
        long rest = index % fileMaxSize;
        if (fileIndex >= files.length)
            files = Arrays.copyOf(files, bufferSize(fileIndex + 1));
        if (files[fileIndex] == null)
            files[fileIndex] = factory.newStorage(new File(directory, fileName(fileIndex)), writable);
        if (filesCount.get() < fileIndex + 1)
            filesCount.set(fileIndex + 1);
        return new RawFileSplitEndpointProxy(
                files[fileIndex],
                files[fileIndex].acquireEndpointAt(rest),
                fileIndex * fileMaxSize,
                fileMaxSize
        );
    }

    @Override
    public void releaseEndpoint(StorageEndpoint endpoint) {
        ((RawFileSplitEndpointProxy) endpoint).release();
    }

    @Override
    public void close() throws IOException {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new IOException("The file is closed");
            if (state.compareAndSet(STATE_READY, STATE_BUSY))
                break;
        }
        try {
            for (int i = 0; i != filesCount.get(); i++) {
                if (files[i] != null)
                    files[i].close();
            }
        } finally {
            state.set(STATE_CLOSED);
        }
    }
}
