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

package fr.cenotelie.commons.storage;

import java.io.IOException;

/**
 * Represents a transactional storage system
 * A transaction storage system is expected to at least provide the following guarantees:
 * - Atomicity, transactions are either fully committed, or not
 * - Isolation, transactions only see the changes of transactions fully committed before they started
 *
 * @author Laurent Wouters
 */
public abstract class TransactionalStorage implements AutoCloseable {
    /**
     * Gets whether this storage system can be written to
     *
     * @return Whether this storage system can be written to
     */
    public abstract boolean isWritable();

    /**
     * Gets the current size of this storage system
     *
     * @return The current size of this storage system
     */
    public abstract long getSize();

    /**
     * Flushes any outstanding changes to this storage system
     *
     * @throws IOException When an IO error occurred
     */
    public abstract void flush() throws IOException;

    /**
     * Starts a new transaction
     * The transaction must be ended by a call to the transaction's close method.
     * The transaction will NOT automatically commit when closed, the commit method should be called before closing.
     *
     * @param writable Whether the transaction shall support writing
     * @return The new transaction
     */
    public Transaction newTransaction(boolean writable) {
        return newTransaction(writable, false);
    }

    /**
     * Starts a new transaction
     * The transaction must be ended by a call to the transaction's close method.
     *
     * @param writable   Whether the transaction shall support writing
     * @param autocommit Whether this transaction should commit when being closed
     * @return The new transaction
     */
    public abstract Transaction newTransaction(boolean writable, boolean autocommit);

    /**
     * Gets the currently running transactions for the current thread
     *
     * @return The current transaction, or null if there is none
     */
    public abstract Transaction getTransaction();

    /**
     * Closes this resource, relinquishing any underlying resources
     *
     * @throws IOException When an IO error occurred
     */
    @Override
    public abstract void close() throws IOException;
}
