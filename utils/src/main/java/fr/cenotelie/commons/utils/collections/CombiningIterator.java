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
 * Representing an iterator computing the cross-product of two collections
 *
 * @param <X> The first type of data to iterate over
 * @param <Y> The second type of data to iterate over
 * @author Laurent Wouters
 */
public class CombiningIterator<X, Y> implements Iterator<Couple<X, Y>> {
    /**
     * The current result for the iterator
     */
    protected final Couple<X, Y> current;
    /**
     * The iterator of values on the left
     */
    protected final Iterator<X> leftIterator;
    /**
     * The next left value for the result
     */
    protected X nextLeft;
    /**
     * The current iterator for the elements on the right
     */
    protected Iterator<Y> rightIterator;
    /**
     * The last iterator on the right used for the last result
     */
    protected Iterator<Y> lastRightIterator;
    /**
     * The adapter to get an iterator of Y for each X item
     */
    protected final Adapter<Iterator<Y>> adapter;

    /**
     * Initializes this iterator
     *
     * @param leftIterator The iterator of values on the left
     * @param adapter      The adapter to get an iterator of Y for each X item
     */
    public CombiningIterator(Iterator<X> leftIterator, Adapter<Iterator<Y>> adapter) {
        this.current = new Couple<>();
        this.leftIterator = leftIterator;
        this.adapter = adapter;
        findNext();
    }

    /**
     * Finds the next result
     */
    private void findNext() {
        while (rightIterator == null || !rightIterator.hasNext()) {
            if (!leftIterator.hasNext()) {
                nextLeft = null;
                rightIterator = null;
                return;
            }
            nextLeft = leftIterator.next();
            rightIterator = adapter.adapt(nextLeft);
        }
    }

    @Override
    public boolean hasNext() {
        return (rightIterator != null && rightIterator.hasNext());
    }

    @Override
    public Couple<X, Y> next() {
        current.x = nextLeft;
        current.y = rightIterator.next();
        lastRightIterator = rightIterator;
        findNext();
        return current;
    }

    @Override
    public void remove() {
        lastRightIterator.remove();
    }
}
