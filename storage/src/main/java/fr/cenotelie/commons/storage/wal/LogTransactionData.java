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
 * Represents the data about a transaction as written in a log
 *
 * @author Laurent Wouters
 */
class LogTransactionData {
    /**
     * Location of this transaction in the log
     */
    public long logLocation;
    /**
     * Sequence number of this transaction
     */
    private final long sequenceNumber;
    /**
     * Timestamp for this transaction
     */
    private final long timestamp;
    /**
     * The data for the pages
     */
    private final LogTransactionPageData[] pages;

    /**
     * Initializes this structure
     *
     * @param sequenceNumber Sequence number of this transaction
     * @param timestamp      Timestamp for this transaction
     * @param pages          The data for the pages
     */
    public LogTransactionData(long sequenceNumber, long timestamp, LogTransactionPageData[] pages) {
        this.sequenceNumber = sequenceNumber;
        this.timestamp = timestamp;
        this.pages = pages;
    }

    /**
     * Initializes this transaction data
     *
     * @param access The access to use for loading
     */
    public LogTransactionData(StorageAccess access) {
        long start = access.getIndex();
        logLocation = access.getLocation() + start;
        sequenceNumber = access.readLong();
        timestamp = access.readLong();
        int count = access.readInt();
        pages = new LogTransactionPageData[count];
        for (int i = 0; i != count; i++) {
            pages[i] = new LogTransactionPageData(access);
        }
    }

    /**
     * Gets the sequence number of this transaction
     *
     * @return The sequence number of this transaction
     */
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * Gets the timestamp for this transaction
     *
     * @return The timestamp for this transaction
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Applies the edits of this transaction to the specified backend
     *
     * @param backend The backend
     */
    public void applyTo(StorageBackend backend) {
        for (LogTransactionPageData page : pages) {
            page.applyTo(backend);
        }
    }

    /**
     * Gets the length of this data in the log
     *
     * @return The length of this data
     */
    public int getLength() {
        int result = 8 + 8 + 4; // seq number, timestamp and pages count
        for (int i = 0; i != pages.length; i++)
            result += pages[i].getLength();
        return result;
    }

    /**
     * Writes this data to the provided access
     *
     * @param access The access to use
     */
    public void writeTo(StorageAccess access) {
        access.writeLong(sequenceNumber);
        access.writeLong(timestamp);
        access.writeInt(pages.length);
        for (int i = 0; i != pages.length; i++)
            pages[i].writeTo(access);
    }

    /**
     * Gets whether this transaction has edits that are concurrent to the specified transaction
     *
     * @param data The data of another transaction
     * @return Whether there are conflicts
     */
    public boolean intersects(LogTransactionData data) {
        for (int i = 0; i != pages.length; i++) {
            for (int j = 0; j != data.pages.length; j++) {
                if (pages[i].location == data.pages[j].location) {
                    // same page
                    if (pages[i].intersects(data.pages[j]))
                        return true;
                    break;
                }
            }
        }
        return false;
    }
}
