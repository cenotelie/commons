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

import fr.cenotelie.commons.storage.Constants;
import fr.cenotelie.commons.storage.Access;
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

    /**
     * Basic test of writing and re-reading
     *
     * @throws IOException When an IO error occurred
     */
    @Test
    public void testBasicWriteAndRereadPageAligned() throws IOException {
        RawFile backend = newBackend(true);
        // we will read and write 2 and a half page to ensure that we trigger the split file mechanism
        int totalLength = Constants.PAGE_SIZE * 3;
        // the number of integer values to read and write
        int valueCount = totalLength / 4;
        try (Access access = backend.access(0, totalLength, true)) {
            for (int i = 0; i != valueCount; i++) {
                access.writeInt(i);
            }
        }
        // check the total size
        Assert.assertEquals(totalLength, backend.getSize());
        // re-read immediately
        try (Access access = backend.access(0, totalLength, false)) {
            for (int i = 0; i != valueCount; i++) {
                int value = access.readInt();
                Assert.assertEquals(i, value);
            }
        }
        // flush and close
        backend.flush();
        Assert.assertEquals(totalLength, backend.getSize());
        backend.close();

        // reopen an re-read in read-only
        backend = reopen(backend, false);
        Assert.assertEquals(totalLength, backend.getSize());
        try (Access access = backend.access(0, totalLength, false)) {
            for (int i = 0; i != valueCount; i++) {
                int value = access.readInt();
                Assert.assertEquals(i, value);
            }
        }
        backend.close();
    }

    /**
     * Basic test of writing and re-reading
     *
     * @throws IOException When an IO error occurred
     */
    @Test
    public void testBasicWriteAndRereadPageUnaligned() throws IOException {
        RawFile backend = newBackend(true);
        // we will read and write 2 and a half page to ensure that we trigger the split file mechanism
        int totalLength = Constants.PAGE_SIZE * 2 + Constants.PAGE_SIZE / 2;
        // the number of integer values to read and write
        int valueCount = totalLength / 4;
        try (Access access = backend.access(0, totalLength, true)) {
            for (int i = 0; i != valueCount; i++) {
                access.writeInt(i);
            }
        }
        // check the total size
        Assert.assertEquals(totalLength, backend.getSize());
        // re-read immediately
        try (Access access = backend.access(0, totalLength, false)) {
            for (int i = 0; i != valueCount; i++) {
                int value = access.readInt();
                Assert.assertEquals(i, value);
            }
        }
        // flush and close
        backend.flush();
        Assert.assertEquals(totalLength, backend.getSize());
        backend.close();

        // reopen an re-read in read-only
        backend = reopen(backend, false);
        Assert.assertEquals(totalLength, backend.getSize());
        try (Access access = backend.access(0, totalLength, false)) {
            for (int i = 0; i != valueCount; i++) {
                int value = access.readInt();
                Assert.assertEquals(i, value);
            }
        }
        backend.close();
    }

    /**
     * Basic test of writing and re-reading
     *
     * @throws IOException When an IO error occurred
     */
    @Test
    public void testTruncatePageAligned() throws IOException {
        RawFile backend = newBackend(true);
        // we will read and write 2 and a half page to ensure that we trigger the split file mechanism
        int totalLength = Constants.PAGE_SIZE * 3;
        // the number of integer values to read and write
        int valueCount = totalLength / 4;
        try (Access access = backend.access(0, totalLength, true)) {
            for (int i = 0; i != valueCount; i++) {
                access.writeInt(i);
            }
        }
        // check the total size
        Assert.assertEquals(totalLength, backend.getSize());
        // flush, truncate and close
        backend.flush();
        totalLength = Constants.PAGE_SIZE;
        valueCount = totalLength / 4;
        backend.truncate(totalLength);
        Assert.assertEquals(totalLength, backend.getSize());
        backend.close();

        // reopen an re-read in read-only
        backend = reopen(backend, false);
        Assert.assertEquals(totalLength, backend.getSize());
        try (Access access = backend.access(0, totalLength, false)) {
            for (int i = 0; i != valueCount; i++) {
                int value = access.readInt();
                Assert.assertEquals(i, value);
            }
        }
        backend.close();
    }

    /**
     * Basic test of writing and re-reading
     *
     * @throws IOException When an IO error occurred
     */
    @Test
    public void testTruncatePageUnaligned() throws IOException {
        RawFile backend = newBackend(true);
        // we will read and write 2 and a half page to ensure that we trigger the split file mechanism
        int totalLength = Constants.PAGE_SIZE * 2 + Constants.PAGE_SIZE / 2;
        // the number of integer values to read and write
        int valueCount = totalLength / 4;
        try (Access access = backend.access(0, totalLength, true)) {
            for (int i = 0; i != valueCount; i++) {
                access.writeInt(i);
            }
        }
        // check the total size
        Assert.assertEquals(totalLength, backend.getSize());
        // flush, truncate and close
        backend.flush();
        totalLength = Constants.PAGE_SIZE + Constants.PAGE_SIZE / 2;
        valueCount = totalLength / 4;
        backend.truncate(totalLength);
        Assert.assertEquals(totalLength, backend.getSize());
        backend.close();

        // reopen an re-read in read-only
        backend = reopen(backend, false);
        Assert.assertEquals(totalLength, backend.getSize());
        try (Access access = backend.access(0, totalLength, false)) {
            for (int i = 0; i != valueCount; i++) {
                int value = access.readInt();
                Assert.assertEquals(i, value);
            }
        }
        backend.close();
    }
}
