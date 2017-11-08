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

import fr.cenotelie.commons.storage.IOEndpoint;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements a conceptually very large file that is physically represented as multiple files on disk
 *
 * @author Laurent Wouters
 */
public class RawSplitFiles extends RawFile {
    /**
     * The factory to use for new individual files
     */
    public interface Factory {
        /**
         * Creates a new file
         *
         * @param file     The physical file to map
         * @param writable Whether the file shall be writable
         * @return The new raw file
         */
        RawFile newFile(File file, boolean writable);
    }

    /**
     * The factory to use for member files
     */
    private final Factory factory;
    /**
     * The directory that contains the member files
     */
    private final File directory;
    /**
     * The prefix for member files
     */
    private final String filePrefix;
    /**
     * The suffix for member files
     */
    private final String fileSuffix;
    /**
     * The list of existing files
     */
    private final List<RawFile> files;
    /**
     * Whether the files are writable
     */
    private final boolean writable;
    /**
     * The mask for indices within a member file
     */
    private final long maskLowerIndex;
    /**
     * The mask for the upper part of indices
     */
    private final long maskUpperIndex;
    /**
     * The maximum length of a member file
     */
    private final long memberFileLength;

    /**
     * Initializes this split files raw store
     *
     * @param factory    The factory to use for member files
     * @param directory  The directory that contains the member files
     * @param filePrefix The prefix for member files
     * @param fileSuffix The suffix for member files
     * @param writable   Whether the files are writable
     * @param lowerMask  The mask for indices within a member file
     */
    public RawSplitFiles(Factory factory, File directory, String filePrefix, String fileSuffix, boolean writable, long lowerMask) {
        this.factory = factory;
        this.directory = directory;
        this.filePrefix = filePrefix;
        this.fileSuffix = fileSuffix;
        this.files = new ArrayList<>();
        this.writable = writable;
        this.maskLowerIndex = lowerMask;
        this.maskUpperIndex = ~lowerMask;
        this.memberFileLength = lowerMask + 1;
    }

    /**
     * Finds
     */
    private void findMembers() {

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
    public void flush() throws IOException {

    }

    @Override
    public IOEndpoint acquireEndpointAt(long index) {
        return null;
    }

    @Override
    public void releaseEndpoint(IOEndpoint endpoint) {

    }

    @Override
    public void close() throws Exception {

    }
}
