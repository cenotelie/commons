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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents an access emitted by a transaction
 *
 * @author Laurent Wouters
 */
class TransactionAccess extends Access {
    /**
     * The page is free, i.e. not assigned to any location
     */
    private static final int STATE_FREE = 0;
    /**
     * The page is reserved, i.e. is going to contain data but is not ready yet
     */
    private static final int STATE_RESERVED = 1;
    /**
     * The page exists and is ready for IO
     */
    private static final int STATE_READY = 3;

    /**
     * The state of this page
     */
    private final AtomicInteger state;

    /**
     * Initializes this access
     */
    public TransactionAccess() {
        super();
        this.state = new AtomicInteger(STATE_FREE);
    }

    /**
     * Tries to reserve this page
     *
     * @return Whether the reservation was successful
     */
    public boolean reserve() {
        return state.compareAndSet(STATE_FREE, STATE_RESERVED);
    }

    /**
     * Setups this access before using it
     *
     * @param backend  The target backend for this access
     * @param location The location of the span for this access within the backend
     * @param length   The length of the allowed span
     * @param writable Whether the access allows writing
     */
    public void init(Storage backend, long location, int length, boolean writable) {
        setup(backend, location, length, writable);
        state.set(STATE_READY);
    }

    @Override
    public void close() {
        releaseOnClose();
        state.set(STATE_FREE);
    }
}
