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

package fr.cenotelie.commons.storage.wal;

import fr.cenotelie.commons.storage.StorageAccess;
import fr.cenotelie.commons.storage.StorageBackend;

/**
 * Represents the data of a paged touched by a transaction in the log
 *
 * @author Laurent Wouters
 */
public class LogPageData extends PageEdits {
    /**
     * The offset of this data relative to the page's location
     */
    public final int offset;
    /**
     * The location in the backend of the touched page
     */
    public final long location;
    /**
     * The content of the edits in the touched page
     */
    private byte[][] editsContent;
    /**
     * The buffer for the content
     */
    private byte[] buffer;

    /**
     * Initializes this structure from the edits of a page
     *
     * @param offset   The offset of this data relative to the page's location
     * @param location The location in the backend of the touched page
     * @param edits    The edits of a page
     */
    public LogPageData(int offset, long location, PageEdits edits, byte[] buffer) {
        this.offset = offset;
        this.location = location;
        this.editsContent = null;
        this.edits = edits.edits;
        this.editsCount = edits.editsCount;
        this.editsContent = null;
        this.buffer = buffer;
    }

    /**
     * Initializes this structure by loading the content from the specified access
     *
     * @param access The access to use for loading
     * @param offset The offset of this data relative to the page's location
     */
    public LogPageData(StorageAccess access, int offset) {
        this.offset = offset;
        this.location = access.readLong();
        this.editsCount = access.readInt();
        this.edits = new long[editsCount];
        this.editsContent = new byte[editsCount][];
        for (int i = 0; i != editsCount; i++) {
            this.edits[i] = access.readLong();
            this.editsContent[i] = access.readBytes(editLength(this.edits[i]));
        }
        this.buffer = null;
    }

    /**
     * Applies the edits of this page to the specified backend
     *
     * @param backend The backend
     */
    public void applyTo(StorageBackend backend) {
        if (editsContent == null)
            return;
        for (int i = 0; i != editsCount; i++) {
            try (StorageAccess access = backend.access(location + editIndex(edits[i]), editLength(edits[i]), true)) {
                access.writeBytes(editsContent[i]);
            }
        }
        editsContent = null;
    }

    /**
     * Gets the length of this data in the log
     *
     * @return The length of this data
     */
    public int getSerializationLength() {
        // uint64: The location of the page
        return 8 + super.getSerializationLength();
    }

    /**
     * Writes this data to the provided access
     *
     * @param access The access to use
     */
    public void writeTo(StorageAccess access) {
        access.writeLong(location);
        access.writeInt(editsCount);
        for (int i = 0; i != editsCount; i++) {
            access.writeLong(edits[i]);
            access.writeBytes(buffer, editIndex(edits[i]), editLength(edits[i]));
        }
        // clear this data
        buffer = null;
    }
}
