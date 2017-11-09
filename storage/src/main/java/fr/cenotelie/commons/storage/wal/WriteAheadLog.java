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

import fr.cenotelie.commons.storage.TSAccessManager;
import fr.cenotelie.commons.storage.raw.RawFile;

/**
 * Implements a write-ahead log that guard the access to another backend storage system
 *
 * @author Laurent Wouters
 */
public class WriteAheadLog {
    /**
     * The backend storage that is guarded by this WAL
     */
    private final RawFile backend;
    /**
     * The raw file for the log itself
     */
    private final RawFile log;
    /**
     * The access manager for the log file
     */
    private final TSAccessManager manager;


    /**
     * Initializes this log
     *
     * @param backend The backend storage that is guarded by this WAL
     * @param log     The raw file for the log itself
     */
    public WriteAheadLog(RawFile backend, RawFile log) {
        this.backend = backend;
        this.log = log;
        this.manager = new TSAccessManager(log);
    }


    /**
     * Starts a new transaction
     *
     * @param writable Whether the transaction shall support writing
     * @return The new transaction
     */
    public Transaction newTransaction(boolean writable) {
        return null;
    }

    /**
     * When the transaction ended
     *
     * @param transaction The transaction that ended
     */
    void onTransactionEnd(Transaction transaction) {

    }

    /**
     * Acquires a page of the backend storage system
     *
     * @param endMark  The sequence number of the last transaction known to the current one
     * @param location The location in the backend storage system of the requested page
     * @param writable Whether the page shall be writable
     * @return The requested page
     */
    Page acquirePage(long endMark, long location, boolean writable) {
        return null;
    }
}
