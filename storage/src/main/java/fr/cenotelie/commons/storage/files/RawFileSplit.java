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
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
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
     * The maximum number of missing file before stopping to look for them
     */
    private static final int MAX_MISSING = 15;

    /**
     * The storage system is ready for IO
     */
    private static final int STATE_READY = 0;
    /**
     * The storage system is currently busy with an operation
     */
    private static final int STATE_BUSY = 1;
    /**
     * The storage system is now closed
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
    private volatile int filesCount;
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
        int last = -1;
        int missing = 0;
        for (int i = 0; missing < MAX_MISSING; i++) {
            File file = new File(directory, fileName(i));
            if (file.exists()) {
                last = i;
                missing = 0;
            } else {
                missing++;
            }
        }
        this.filesCount = last + 1;
        this.files = new RawFile[bufferSize(last + 1)];
        this.state = new AtomicInteger(STATE_READY);
    }

    /**
     * Gets the size of the buffer (power of 2) that can hold the specified count
     *
     * @param count A number of files
     * @return The size of the buffer
     */
    private static int bufferSize(int count) {
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
            if (filesCount == 0)
                return 0;
            long total = (filesCount - 1) * fileMaxSize;
            if (files[filesCount - 1] == null)
                files[filesCount - 1] = factory.newStorage(new File(directory, fileName(filesCount - 1)), writable);
            total += files[filesCount - 1].getSize();
            return total;
        } catch (IOException exception) {
            throw new RuntimeException(exception);
        } finally {
            state.set(STATE_READY);
        }
    }

    @Override
    public boolean cut(long from, long to) throws IOException {
        if (from < 0 || from > to)
            throw new IndexOutOfBoundsException();
        if (from == to)
            // 0-length cut => do nothing
            return false;
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new IllegalStateException();
            if (state.compareAndSet(STATE_READY, STATE_BUSY))
                break;
        }
        try {
            return doCut(from, to);
        } finally {
            state.set(STATE_READY);
        }
    }

    /**
     * Cuts content within this storage system
     *
     * @param from The starting index to cut at (included)
     * @param to   The end index to cut to (excluded)
     * @return Whether the operation had an effect
     * @throws IOException When an IO error occurred
     */
    private boolean doCut(long from, long to) throws IOException {
        long fromFileIndex = from / fileMaxSize;
        long fromRest = from % fileMaxSize;
        long toFileIndex = to / fileMaxSize;
        long toRest = to % fileMaxSize;

        if (fromFileIndex >= filesCount)
            // nothing to cut
            return false;
        // re-adjust the end of the cut if necessary
        if (toFileIndex > filesCount) {
            toFileIndex = filesCount;
            toRest = 0;
        }
        if (toRest == 0) {
            toFileIndex--;
            toRest = fileMaxSize;
        }

        try {
            // cut the first file
            boolean didSomething = doCutFile((int) fromFileIndex, fromRest, toFileIndex == fromFileIndex ? toRest : fileMaxSize);
            if (fromFileIndex == toFileIndex)
                return didSomething;
            for (int i = (int) fromFileIndex + 1; i != toFileIndex; i++) {
                // not the last file
                didSomething |= doCutFile(i, 0, fileMaxSize);
            }
            // cut the last file
            didSomething |= doCutFile((int) toFileIndex, 0, toRest);
            return didSomething;
        } finally {
            doCutDetectLastFile();
        }
    }

    /**
     * Cuts a specific file
     *
     * @param fileIndex The index of the file to cut
     * @param from      The starting index to cut at (included)
     * @param to        The end index to cut to (excluded)
     * @return Whether the operation had an effect
     * @throws IOException When an IO error occurred
     */
    private boolean doCutFile(int fileIndex, long from, long to) throws IOException {
        if (fileIndex >= filesCount)
            // this file does not exist
            return false;
        if (files[fileIndex] != null) {
            // the file is open, apply the cut
            return doCutFileOpen(fileIndex, from, to);
        }

        File target = new File(directory, fileName(fileIndex));
        if (!target.exists())
            // the file does not exist
            return false;
        long length = target.length();
        if (from >= length)
            // the cut is after the file
            return false;
        if (to >= length) {
            // this would either completely delete the file, or truncate it
            if (from == 0) {
                // delete the file
                if (target.exists() && !target.delete())
                    throw new IOException("Failed to delete file " + target.getAbsolutePath());
                return true;
            }
            // here, truncate at from
            try (FileChannel channel = FileChannel.open(target.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                channel.truncate(from);
                channel.force(true);
            }
            return true;
        }

        // here, the cut is within the file, we need to allocate the file
        files[fileIndex] = factory.newStorage(target, writable);
        return doCutFileOpen(fileIndex, from, to);
    }

    /**
     * Cuts a specific file when it is open
     *
     * @param fileIndex The index of the file to cut
     * @param from      The starting index to cut at (included)
     * @param to        The end index to cut to (excluded)
     * @return Whether the operation had an effect
     * @throws IOException When an IO error occurred
     */
    private boolean doCutFileOpen(int fileIndex, long from, long to) throws IOException {
        boolean result = files[fileIndex].cut(from, to);
        if (!result)
            // did nothing
            return false;
        if (from == 0 && files[fileIndex].getSize() == 0) {
            // file is now empty, delete it
            files[fileIndex].close();
            files[fileIndex] = null;
            File target = new File(directory, fileName(fileIndex));
            if (target.exists() && !target.delete())
                throw new IOException("Failed to delete file " + target.getAbsolutePath());
        }
        return true;
    }

    /**
     * (Re-) detects the last file after the cut
     */
    private void doCutDetectLastFile() {
        for (int i = filesCount - 1; i != -1; i--) {
            File file = new File(directory, fileName(i));
            if (file.exists()) {
                // this file exists, this is the last file
                filesCount = i + 1;
                return;
            }
        }
    }

    @Override
    public void flush() throws IOException {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new IllegalStateException();
            if (state.compareAndSet(STATE_READY, STATE_BUSY))
                break;
        }
        try {
            for (int i = 0; i != filesCount; i++) {
                if (files[i] != null)
                    files[i].flush();
            }
        } finally {
            state.set(STATE_READY);
        }
    }

    @Override
    public Endpoint acquireEndpointAt(long index) {
        if (index < 0)
            throw new IndexOutOfBoundsException();
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new IllegalStateException();
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
     * Acquires an endpoint that enables reading and writing to the storage system at the specified index
     * The endpoint must be subsequently released by a call to
     *
     * @param index An index within this storage system
     * @return The corresponding endpoint
     */
    private Endpoint doAcquireEndpointAt(long index) throws IOException {
        int fileIndex = (int) (index / fileMaxSize);
        long rest = index % fileMaxSize;
        if (fileIndex >= files.length)
            files = Arrays.copyOf(files, bufferSize(fileIndex + 1));
        if (files[fileIndex] == null)
            files[fileIndex] = factory.newStorage(new File(directory, fileName(fileIndex)), writable);
        if (filesCount < fileIndex + 1)
            filesCount = fileIndex + 1;
        return new RawFileSplitEndpointProxy(
                files[fileIndex],
                files[fileIndex].acquireEndpointAt(rest),
                fileIndex * fileMaxSize,
                fileMaxSize
        );
    }

    @Override
    public void releaseEndpoint(Endpoint endpoint) {
        ((RawFileSplitEndpointProxy) endpoint).release();
    }

    @Override
    public void close() throws IOException {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED)
                throw new IllegalStateException();
            if (state.compareAndSet(STATE_READY, STATE_BUSY))
                break;
        }
        try {
            for (int i = 0; i != filesCount; i++) {
                if (files[i] != null)
                    files[i].close();
            }
        } finally {
            state.set(STATE_CLOSED);
        }
    }
}
