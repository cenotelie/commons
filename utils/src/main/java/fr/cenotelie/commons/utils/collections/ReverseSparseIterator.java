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
 * Represents a reverse iterator over an array that may contain null values
 * This iterator goes from the end of the array to the front
 *
 * @param <T> The type of the data to iterate over
 * @author Laurent Wouters
 */
public class ReverseSparseIterator<T> implements Iterator<T> {
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
    public ReverseSparseIterator(T[] content) {
        this.content = content;
        this.index = content.length - 1;
        this.lastIndex = -1;
        while (index != -1 && content[index] == null)
            index--;
    }

    @Override
    public boolean hasNext() {
        return (index != -1);
    }

    @Override
    public T next() {
        lastIndex = index;
        T result = content[index];
        index--;
        while (index != -1 && content[index] == null)
            index--;
        return result;
    }

    @Override
    public void remove() {
        content[lastIndex] = null;
    }
}
