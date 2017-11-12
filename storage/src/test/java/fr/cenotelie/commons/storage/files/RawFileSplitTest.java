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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Test suite for a raw file storage system
 *
 * @author Laurent Wouters
 */
public class RawFileSplitTest extends RawFileTest {
    /**
     * Maximum size of split files
     */
    private static final int MAX_SIZE = Constants.PAGE_SIZE * 2;

    @Override
    protected RawFile newBackend(boolean writable) throws IOException {
        Path directory = Files.createTempDirectory("test");
        return new RawFileSplit(
                directory.toFile(),
                "test",
                ".bin",
                new RawFileFactory() {
                    @Override
                    public RawFile newStorage(File file, boolean writable) throws IOException {
                        return new RawFileDirect(file, writable);
                    }
                },
                writable,
                MAX_SIZE
        );
    }

    @Override
    protected RawFile reopen(RawFile old, boolean writable) throws IOException {
        return new RawFileSplit(
                old.getSystemFile(),
                "test",
                ".bin",
                new RawFileFactory() {
                    @Override
                    public RawFile newStorage(File file, boolean writable) throws IOException {
                        return new RawFileDirect(file, writable);
                    }
                },
                writable,
                MAX_SIZE
        );
    }
}
