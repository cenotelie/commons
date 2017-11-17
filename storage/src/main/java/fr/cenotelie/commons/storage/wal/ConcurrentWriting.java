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
public class ConcurrentWriting extends IOException {
    /**
     * The first conflicting transaction found
     */
    private final LogTransactionData conflicting;

    /**
     * Gets the first conflicting transaction found
     *
     * @return The first conflicting transaction found
     */
    public LogTransactionData getConflicting() {
        return conflicting;
    }

    /**
     * Initializes this exception
     *
     * @param conflicting The first conflicting transaction found
     */
    public ConcurrentWriting(LogTransactionData conflicting) {
        this.conflicting = conflicting;
    }
}
