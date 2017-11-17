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
public class LogTransactionPageData {
    /**
     * The location in the backend of the touched page
     */
    public final long location;
    /**
     * The offsets of the edits in the touched page
     */
    public final int[] editsOffsets;
    /**
     * The lengths of the edits in the touched page
     */
    public final int[] editsLength;
    /**
     * The content of the edits in the touched page
     */
    public byte[][] editsContent;

    /**
     * Initializes this structure as empty
     *
     * @param location     The location in the backend of the touched page
     * @param editsOffsets The offsets of the edits in the touched page
     * @param editsContent The content of the edits in the touched page
     */
    public LogTransactionPageData(long location, int[] editsOffsets, byte[][] editsContent) {
        this.location = location;
        this.editsOffsets = editsOffsets;
        this.editsLength = new int[editsOffsets.length];
        this.editsContent = editsContent;
        for (int i = 0; i != editsContent.length; i++)
            editsLength[i] = editsContent.length;
    }

    /**
     * Initializes this structure by loading the content from the specified access
     *
     * @param access The access to use for loading
     */
    public LogTransactionPageData(StorageAccess access) {
        this.location = access.readLong();
        int editsCount = access.readInt();
        this.editsOffsets = new int[editsCount];
        this.editsLength = null;
        this.editsContent = new byte[editsCount][];
        for (int i = 0; i != editsCount; i++) {
            editsOffsets[i] = access.readInt();
            int length = access.readInt();
            editsContent[i] = access.readBytes(length);
        }
    }

    /**
     * Applies the edits of this page to the specified backend
     *
     * @param backend The backend
     */
    public void applyTo(StorageBackend backend) {
        if (editsContent == null)
            return;
        for (int i = 0; i != editsOffsets.length; i++) {
            try (StorageAccess access = backend.access(location + editsOffsets[i], editsContent[i].length, true)) {
                access.writeBytes(editsContent[i]);
            }
        }
    }

    /**
     * Gets the length of this data in the log
     *
     * @return The length of this data
     */
    public int getLength() {
        int result = 8 + 4; // location and edits count
        for (int i = 0; i != editsLength.length; i++) {
            result += 4 + 4 + editsLength[i]; // offset, edit's length and edit's content
        }
        return result;
    }

    /**
     * Writes this data to the provided access
     *
     * @param access The access to use
     */
    public void writeTo(StorageAccess access) {
        access.writeLong(location);
        access.writeInt(editsOffsets.length);
        for (int i = 0; i != editsOffsets.length; i++) {
            access.writeInt(editsOffsets[i]);
            access.writeInt(editsContent[i].length);
            access.writeBytes(editsContent[i]);
        }
        // clear this data
        editsContent = null;
    }

    /**
     * Gets whether this page has edits that are concurrent to the specified page
     *
     * @param data The data of another page
     * @return Whether there are conflicts
     */
    public boolean intersects(LogTransactionPageData data) {
        // TODO: implements this
        return false;
    }
}
