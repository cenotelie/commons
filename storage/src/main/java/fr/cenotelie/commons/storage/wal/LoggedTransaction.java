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

/**
 * Represents the data of a transaction that appears in the log
 *
 * @author Laurent Wouters
 */
class LoggedTransaction {
    /**
     * The sequence number for this transaction
     */
    public final long sequenceNumber;
    /**
     * The locations of the pages modified by this transaction in the backend storage
     */
    public final long[] pageLocations;
    /**
     * The offsets from the transaction's location to the data about the page's modification in the log
     */
    public final int[] pageOffsets;
    /**
     * The location of this transaction in the log
     */
    public long location;
}
