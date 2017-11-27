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

package fr.cenotelie.commons.storage.stores;

import fr.cenotelie.commons.storage.Access;

/**
 * Implements a serialization and de-serialization mediator for a class of objects
 *
 * @param <T> The type of objects
 * @author Laurent Wouters
 */
public abstract class ObjectMediator<T> {
    /**
     * Gets the serialization size of the objects
     *
     * @return The serialization size of the objects
     */
    public abstract int getSerializationSize();

    /**
     * Serializes an object
     *
     * @param access The access to write with
     * @param object The object to serialize
     */
    public abstract void write(Access access, T object);

    /**
     * De-serialized an object
     *
     * @param access The access to read from
     * @return The de-serialized object
     */
    public abstract T read(Access access);
}
