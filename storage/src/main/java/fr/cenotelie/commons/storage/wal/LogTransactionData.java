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
 * Represents the data about a transaction in a log
 *
 * @author Laurent Wouters
 */
class LogTransactionData {
    /**
     * Data about a touched page
     */
    public static class Page {
        /**
         * The offset of the data from the location of the transaction in the log
         */
        public final int offset;
        /**
         * The location in the backend of the touched page
         */
        public final long location;
        /**
         * The number of edits in the touched page
         */
        public final int editsCount;
        /**
         * The offsets of the edits in the touched page
         */
        public int[] editsOffsets;
        /**
         * The content of the edits in the touched page
         */
        public byte[][] editsContent;

        /**
         * Initializes page
         *
         * @param access        The access to use for loading
         * @param loadIndexOnly Whether to load indices only
         * @param start         The index of the parent transaction in the given access
         */
        public Page(StorageAccess access, boolean loadIndexOnly, long start) {
            this.offset = (int) (start - access.getIndex());
            this.location = access.readLong();
            this.editsCount = access.readInt();
            if (loadIndexOnly)
                return;
            this.editsOffsets = new int[editsCount];
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
            if (editsOffsets == null)
                return;
            for (int i = 0; i != editsCount; i++) {
                try (StorageAccess access = new StorageAccess(backend, location + editsOffsets[i], editsContent[i].length, true)) {
                    access.writeBytes(editsContent[i]);
                }
            }
        }
    }

    /**
     * Location of this transaction in the log
     */
    private long logLocation;
    /**
     * Sequence number of this transaction
     */
    private final long sequenceNumber;
    /**
     * The data for the pages
     */
    private final Page[] pages;

    /**
     * Initializes this transaction data
     *
     * @param access        The access to use for loading
     * @param loadIndexOnly Whether to load indices only
     */
    public LogTransactionData(StorageAccess access, boolean loadIndexOnly) {
        long start = access.getIndex();
        logLocation = access.getLocation() + start;
        sequenceNumber = access.readLong();
        int count = access.readInt();
        pages = new Page[count];
        for (int i = 0; i != count; i++) {
            pages[i] = new Page(access, loadIndexOnly, start);
        }
    }

    /**
     * Applies the edits of this transaction to the specified backend
     *
     * @param backend The backend
     */
    public void applyTo(StorageBackend backend) {
        for (Page page : pages) {
            page.applyTo(backend);
        }
    }
}
