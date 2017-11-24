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

import java.io.IOException;

/**
 * Exception raised when at the moment of committing a transaction to a write-ahead log,
 * it appears there are some conflicts with the edits made by another transaction.
 *
 * @author Laurent Wouters
 */
public class ConcurrentWritingException extends IOException {
    /**
     * The sequence number of the conflicting transaction
     */
    private final long sequenceNumber;
    /**
     * The timestamp of the conflicting transaction
     */
    private final long timestamp;

    /**
     * Gets the sequence number of the conflicting transaction
     *
     * @return The sequence number of the conflicting transaction
     */
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * Gets the timestamp of the conflicting transaction
     *
     * @return The timestamp of the conflicting transaction
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Initializes this exception
     *
     * @param sequenceNumber The sequence number of the conflicting transaction
     * @param timestamp      The timestamp of the conflicting transaction
     */
    public ConcurrentWritingException(long sequenceNumber, long timestamp) {
        this.sequenceNumber = sequenceNumber;
        this.timestamp = timestamp;
    }
}
