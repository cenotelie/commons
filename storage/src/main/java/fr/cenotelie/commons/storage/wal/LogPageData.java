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

import fr.cenotelie.commons.storage.Access;
import fr.cenotelie.commons.storage.Storage;

/**
 * Represents the data of a paged touched by a transaction in the log
 *
 * @author Laurent Wouters
 */
public class LogPageData extends PageEdits {
    /**
     * The serialization size of the page data header:
     * - int64: the location of the page
     * - The edits header
     */
    public static final int SERIALIZATION_SIZE_HEADER = 8 + PageEdits.SERIALIZATION_SIZE_HEADER;

    /**
     * The offset of this data relative to the page's location
     */
    public final int offset;
    /**
     * The location in the backing storage system of the touched page
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
     * @param location The location in the backing storage system of the touched page
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
    public LogPageData(Access access, int offset) {
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
     * Loads the content of the edits
     *
     * @param access The access to use for loading
     */
    public void loadContent(Access access) {
        editsContent = new byte[editsCount][];
        access.skip(SERIALIZATION_SIZE_HEADER); // skip the location data and number of edits
        for (int i = 0; i != editsCount; i++) {
            int length = PageEdits.editLength(edits[i]);
            access.skip(PageEdits.SERIALIZATION_SIZE_EDIT_HEADER); // skip the offset and length
            editsContent[i] = access.readBytes(length);
        }
    }

    /**
     * Applies the edits of this page to the backing storage system
     *
     * @param storage The backing storage system
     */
    public void applyTo(Storage storage) {
        if (editsContent == null)
            return;
        for (int i = 0; i != editsCount; i++) {
            try (Access access = storage.access(location + editIndex(edits[i]), editLength(edits[i]), true)) {
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
     * Serialize this data into a log through the provided access
     *
     * @param access The access to use
     */
    public void serialize(Access access) {
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
