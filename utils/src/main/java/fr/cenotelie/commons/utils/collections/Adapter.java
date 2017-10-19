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

package fr.cenotelie.commons.utils.collections;

/**
 * Adapts elements from one type to another
 *
 * @param <T> The type to adapt to
 * @author Laurent Wouters
 */
public interface Adapter<T> {
    /**
     * Adapts the specified element
     *
     * @param element The element to adapt
     * @param <X>     The type of the element to adapt
     * @return The adapted element
     */
    <X> T adapt(X element);
}
