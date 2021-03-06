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
 * Represents an iterator over an array that may contain null values
 *
 * @param <T> The type of the data to iterate over
 * @author Laurent Wouters
 */
public class SparseIterator<T> implements Iterator<T> {
    /**
     * The content to iterate over
     */
    protected final T[] content;
    /**
     * The current index
     */
    protected int index;
    /**
     * The index of the last result
     */
    protected int lastIndex;

    /**
     * Initializes this iterator
     *
     * @param content The content to iterate over
     */
    public SparseIterator(T[] content) {
        this.content = content;
        this.index = 0;
        this.lastIndex = -1;
        while (index != content.length && content[index] == null)
            index++;
    }

    @Override
    public boolean hasNext() {
        return (index != content.length);
    }

    @Override
    public T next() {
        lastIndex = index;
        T result = content[index];
        index++;
        while (index != content.length && content[index] == null)
            index++;
        return result;
    }

    @Override
    public void remove() {
        content[lastIndex] = null;
    }
}
