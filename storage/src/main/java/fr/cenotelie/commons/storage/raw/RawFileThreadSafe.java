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

import fr.cenotelie.commons.storage.IOAccess;
import fr.cenotelie.commons.storage.IOEndpoint;
import fr.cenotelie.commons.storage.TSAccessManager;

import java.io.File;
import java.io.IOException;

/**
 * Implements a thread-safe proxy for a raw file that ensures that no writing thread can overlap with other threads accessing the same file
 *
 * @author Laurent Wouters
 */
public class RawFileThreadSafe extends RawFile {
    /**
     * The backend raw file
     */
    private final RawFile backend;
    /**
     * The access manager to use
     */
    private final TSAccessManager accessManager;

    /**
     * Initializes this structure
     *
     * @param backend The backend raw file
     */
    public RawFileThreadSafe(RawFile backend) {
        this.backend = backend;
        this.accessManager = new TSAccessManager(backend);
    }

    @Override
    public File getSystemFile() {
        return backend.getSystemFile();
    }

    @Override
    public boolean isWritable() {
        return backend.isWritable();
    }

    @Override
    public long getSize() {
        return backend.getSize();
    }

    @Override
    public void flush() throws IOException {
        backend.flush();
    }

    @Override
    public IOEndpoint acquireEndpointAt(long index) {
        throw new UnsupportedOperationException("This operation is not allowed");
    }

    @Override
    public void releaseEndpoint(IOEndpoint endpoint) {
        throw new UnsupportedOperationException("This operation is not allowed");
    }

    @Override
    public void close() throws IOException {
        backend.close();
    }

    /**
     * Accesses the content of this file through an access element
     * An access must be within the boundaries of a block.
     *
     * @param index    The index within this file of the reserved area for the access
     * @param length   The length of the reserved area for the access
     * @param writable Whether the access shall allow writing
     * @return The access element
     */
    public IOAccess access(int index, int length, boolean writable) {
        return accessManager.get(index, length, backend.isWritable() && writable);
    }
}
