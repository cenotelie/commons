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

import java.io.File;
import java.io.IOException;

/**
 * Factory to create file storage backend for specified files
 *
 * @author Laurent Wouters
 */
public abstract class RawFileFactory {
    /**
     * Creates a file storage system for the specified file
     *
     * @param file     The system file
     * @param writable Whether the backend shall allow writing
     * @return The file storage system
     * @throws IOException when an IO error occurred
     */
    public abstract RawFile newStorage(File file, boolean writable) throws IOException;

    /**
     * The default factory
     */
    public static final RawFileFactory DEFAULT = new RawFileFactory() {
        @Override
        public RawFile newStorage(File file, boolean writable) throws IOException {
            return new RawFileBuffered(file, writable);
        }
    };
}
