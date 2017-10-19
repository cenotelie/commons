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
 * Represents an iterator over the indices of an array, skipping null entries
 *
 * @param <T> The type of the input data
 * @author Laurent Wouters
 */
public class IndexIterator<T> implements Iterator<Integer> {
    /**
     * The content to iterate over
     */
    protected final T[] content;
    /**
     * The current index indicating the next result
     */
    protected int index;
    /**
     * The last result
     */
    protected int lastResult;

    /**
     * Initializes this iterator
     *
     * @param content The content to iterate over
     */
    public IndexIterator(T[] content) {
        this.content = content;
        this.index = 0;
        this.lastResult = -1;
        while (index != content.length && content[index] == null)
            index++;
    }

    @Override
    public boolean hasNext() {
        return (index != content.length);
    }

    @Override
    public Integer next() {
        lastResult = index;
        index++;
        while (index != content.length && content[index] == null)
            index++;
        return lastResult;
    }

    @Override
    public void remove() {
        content[lastResult] = null;
    }
}
