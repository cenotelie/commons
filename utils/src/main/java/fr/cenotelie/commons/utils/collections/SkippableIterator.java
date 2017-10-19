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
 * Represents an iterator that adapts another while skipping over null values
 *
 * @param <T> The type of the data to iterate over
 * @author Laurent Wouters
 */
public class SkippableIterator<T> implements Iterator<T> {
    /**
     * The original iterator
     */
    protected final Iterator<T> original;
    /**
     * The next item
     */
    protected T nextItem;
    /**
     * Flag whether the next result must be looked for
     */
    protected boolean mustFindNext;

    /**
     * Initializes this iterator
     *
     * @param original The original iterator
     */
    public SkippableIterator(Iterator<T> original) {
        this.original = original;
        this.mustFindNext = true;
    }

    /**
     * Finds the next element for this iterator
     */
    private void findNext() {
        while (original.hasNext() && nextItem == null) {
            nextItem = original.next();
        }
        mustFindNext = false;
    }

    @Override
    public boolean hasNext() {
        if (mustFindNext)
            findNext();
        return (nextItem != null);
    }

    @Override
    public T next() {
        if (mustFindNext)
            findNext();
        T result = nextItem;
        nextItem = null;
        mustFindNext = true;
        return result;
    }

    @Override
    public void remove() {
        if (!mustFindNext)
            throw new IllegalStateException("The hasNext method must have been not called for this operation to succeed");
        original.remove();
    }
}
