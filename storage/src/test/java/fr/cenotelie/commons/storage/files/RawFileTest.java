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

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Test suite for a raw file storage system
 *
 * @author Laurent Wouters
 */
public abstract class RawFileTest {
    /**
     * Creates a new backend to test with
     *
     * @param writable Whether the storage shall be writable
     * @return The new backend
     * @throws IOException When an IO error occurred
     */
    protected abstract RawFile newBackend(boolean writable) throws IOException;

    /**
     * Reopens an old backend
     *
     * @param old      The old backend
     * @param writable Whether the storage shall be writable
     * @return The new backend
     * @throws IOException When an IO error occurred
     */
    protected abstract RawFile reopen(RawFile old, boolean writable) throws IOException;

    /**
     * Test the initial size of the storage
     *
     * @throws IOException When an IO error occurred
     */
    @Test
    public void testInitialSize() throws IOException {
        RawFile backend = newBackend(true);
        Assert.assertEquals("The backend is not empty", 0, backend.getSize());
        backend.close();
        backend = reopen(backend, true);
        Assert.assertEquals("The backend is not empty", 0, backend.getSize());
        backend.close();
    }
}
