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
 * Represents an access emitted by a transaction
 *
 * @author Laurent Wouters
 */
class TransactionAccess extends StorageAccess {
    /**
     * The parent write-ahead log
     */
    private final WriteAheadLog log;
    /**
     * The identifier of this access for the parent log
     */
    final int identifier;

    /**
     * Initializes this access
     *
     * @param log        The parent write-ahead log
     * @param identifier The identifier of this access for the parent log
     */
    public TransactionAccess(WriteAheadLog log, int identifier) {
        super();
        this.log = log;
        this.identifier = identifier;
    }

    /**
     * Setups this access before using it
     *
     * @param backend  The target backend for this access
     * @param location The location of the span for this access within the backend
     * @param length   The length of the allowed span
     * @param writable Whether the access allows writing
     */
    public void init(StorageBackend backend, long location, int length, boolean writable) {
        this.setup(backend, location, length, writable);
    }

    @Override
    public void close() {
        releaseOnClose();
        log.onAccessEnd(this);
    }
}
