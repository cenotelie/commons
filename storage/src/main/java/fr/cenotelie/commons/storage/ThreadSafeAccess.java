/*******************************************************************************
 * Copyright (c) 2016 Association Cénotélie (cenotelie.fr)
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

/**
 * Represents a thread-safe access to a storage system
 *
 * @author Laurent Wouters
 */
class ThreadSafeAccess extends Access {
    /**
     * The parent manager
     */
    private final ThreadSafeAccessManager manager;
    /**
     * The identifier of this access for the parent manager
     */
    final int identifier;

    /**
     * Initializes this access
     *
     * @param manager    The parent manager
     * @param identifier The identifier of this access for the parent manager
     */
    public ThreadSafeAccess(ThreadSafeAccessManager manager, int identifier) {
        super();
        this.manager = manager;
        this.identifier = identifier;
    }

    /**
     * Initializes this access
     *
     * @param manager    The parent manager
     * @param identifier The identifier of this access for the parent manager
     */
    public ThreadSafeAccess(ThreadSafeAccessManager manager, int identifier, int location) {
        super(null, location, 0, false);
        this.manager = manager;
        this.identifier = identifier;
    }

    @Override
    public void close() {
        releaseOnClose();
        manager.onAccessEnd(this);
    }
}
