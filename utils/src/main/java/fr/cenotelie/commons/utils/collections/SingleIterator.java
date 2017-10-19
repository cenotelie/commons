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

import java.util.Iterator;

/**
 * Represents an iterator over a single instance
 *
 * @param <T> The type of the data to iterate over
 * @author Laurent Wouters
 */
public class SingleIterator<T> implements Iterator<T> {
    /**
     * The content to iterate over
     */
    private T content;

    /**
     * Initializes this iterator
     *
     * @param content The content to iterate over
     */
    public SingleIterator(T content) {
        this.content = content;
    }

    @Override
    public boolean hasNext() {
        return (content != null);
    }

    @Override
    public T next() {
        T result = content;
        content = null;
        return result;
    }

    @Override
    public void remove() {
        content = null;
    }
}
