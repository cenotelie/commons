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
 * Represents a thread-safe access to a backend
 *
 * @author Laurent Wouters
 */
class TSAccess extends IOAccess {
    /**
     * The parent manager
     */
    private final TSAccessManager manager;
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
    TSAccess(TSAccessManager manager, int identifier) {
        this.manager = manager;
        this.identifier = identifier;
    }

    @Override
    public void close() {
        manager.onAccessEnd(this);
    }
}
