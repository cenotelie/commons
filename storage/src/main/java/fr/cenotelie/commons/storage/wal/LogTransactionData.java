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
    private final LogPageData[] pages;

    /**
     * Initializes this structure
     *
     * @param sequenceNumber Sequence number of this transaction
     * @param timestamp      Timestamp for this transaction
     * @param pages          The data for the pages
     */
    public LogTransactionData(long sequenceNumber, long timestamp, LogPageData[] pages) {
        this.sequenceNumber = sequenceNumber;
        this.timestamp = timestamp;
        this.pages = pages;
    }

    /**
     * Initializes this transaction data
     *
     * @param access The access to use for loading
     */
    public LogTransactionData(Access access) {
        long start = access.getIndex();
        logLocation = access.getLocation() + start;
        sequenceNumber = access.readLong();
        timestamp = access.readLong();
        int count = access.readInt();
        pages = new LogPageData[count];
        for (int i = 0; i != count; i++) {
            long current = access.getIndex();
            pages[i] = new LogPageData(access, (int) (current - start));
        }
    }

    /**
     * Loads the content of the transaction
     *
     * @param access The access to use for loading
     */
    public void loadContent(Access access) {
        access.skip(8 + 8 + 4); // seq number, timestamp and pages count
        for (int i = 0; i != pages.length; i++)
            pages[i].loadContent(access);
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
     * Gets the data for the specified page, or null of there is none
     *
     * @param location The location of the requested page
     * @return The data for the page, or null if there is none
     */
    public LogPageData getPage(long location) {
        for (int i = 0; i != pages.length; i++) {
            if (pages[i].location == location)
                return pages[i];
        }
        return null;
    }

    /**
     * Applies the edits of this transaction to the specified backend
     *
     * @param backend The backend
     */
    public void applyTo(Storage backend) {
        for (LogPageData page : pages) {
            page.applyTo(backend);
        }
    }

    /**
     * Gets the length of this data in the log
     *
     * @return The length of this data
     */
    public int getSerializationLength() {
        int result = 8 + 8 + 4; // seq number, timestamp and pages count
        for (int i = 0; i != pages.length; i++)
            result += pages[i].getSerializationLength();
        return result;
    }

    /**
     * Writes this data to the provided access
     *
     * @param access The access to use
     */
    public void writeTo(Access access) {
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
