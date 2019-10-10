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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * Represents a buffer of T elements that is fast when removing elements from it.
 * This class is NOT thread-safe when inserting and removing elements.
 * However, due to its inner representation, the provided iterator is thread-safe.
 *
 * @author Laurent Wouters
 */
public class FastBuffer<T> implements Collection<T> {
    /**
     * The internal representation
     */
    private T[] inner;
    /**
     * The number of elements in this buffer
     */
    private int size;

    /**
     * Initializes this buffer
     *
     * @param capacity The initial capacity
     */
    public FastBuffer(int capacity) {
        this.inner = (T[]) (new Object[capacity]);
        this.size = 0;
    }

    /**
     * Initializes this buffer with the specified elements
     *
     * @param init The original elements
     */
    public FastBuffer(Collection<T> init) {
        if (init instanceof FastBuffer)
            copy((FastBuffer<T>) init);
        else if (init instanceof ArrayList) {
            this.inner = (T[]) init.toArray();
            this.size = init.size();
        } else {
            this.inner = (T[]) (new Object[init.size()]);
            this.size = 0;
            int i = 0;
            for (T elem : init) {
                if (elem == null)
                    continue;
                this.inner[i] = elem;
                i++;
                size++;
            }
        }
    }

    /**
     * Copies the specified buffer into this one
     *
     * @param original The buffer to copy
     */
    private void copy(FastBuffer<T> original) {
        this.inner = Arrays.copyOf(original.inner, original.inner.length);
        this.size = original.size;
    }

    @Override
    public Iterator<T> iterator() {
        return new SparseIterator<T>(inner) {
            @Override
            public void remove() {
                T previous = content[lastIndex];
                content[lastIndex] = null;
                size -= (previous == null ? 0 : 1);
            }
        };
    }

    /**
     * Gets an iterator over this buffer that iterate in the reverse order
     *
     * @return A reverse iterator for this buffer
     */
    public Iterator<T> reverseIterator() {
        return new ReverseSparseIterator<T>(inner) {
            @Override
            public void remove() {
                T previous = content[lastIndex];
                content[lastIndex] = null;
                size -= (previous == null ? 0 : 1);
            }
        };
    }

    /**
     * Gets the first element of this buffer
     *
     * @return The first element of this buffer
     */
    public T first() {
        if (size == 0)
            return null;
        for (int i = 0; i != inner.length; i++) {
            T candidate = inner[i];
            if (candidate != null)
                return candidate;
        }
        return null;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return (size == 0);
    }

    @Override
    public boolean contains(Object o) {
        if (size == 0)
            return false;
        for (int i = 0; i != inner.length; i++)
            if (inner[i] == o)
                return true;
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> clctn) {
        for (Object element : clctn) {
            if (!contains(element))
                return false;
        }
        return true;
    }

    @Override
    public boolean add(T e) {
        if (inner.length == size) {
            int index = inner.length;
            inner = Arrays.copyOf(inner, inner.length * 2);
            inner[index] = e;
            size++;
            return true;
        }
        for (int i = 0; i != inner.length; i++) {
            if (inner[i] == null) {
                inner[i] = e;
                size++;
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean remove(Object o) {
        if (size == 0)
            return false;
        for (int i = 0; i != inner.length; i++) {
            if (inner[i] == o) {
                inner[i] = null;
                size--;
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends T> clctn) {
        for (T element : clctn) {
            add(element);
        }
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> clctn) {
        for (Object element : clctn) {
            remove(element);
        }
        return true;
    }

    @Override
    public boolean retainAll(Collection<?> clctn) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        for (int i = 0; i != inner.length; i++)
            inner[i] = null;
        size = 0;
    }

    @Override
    public Object[] toArray() {
        return toArray(inner);
    }

    @Override
    public <X> X[] toArray(X[] ts) {
        X[] temp = (X[]) Array.newInstance(ts.getClass().getComponentType(), size);
        int index = 0;
        for (int i = 0; i != inner.length; i++) {
            if (inner[i] != null) {
                temp[index] = (X) inner[i];
                index++;
            }
        }
        return temp;
    }
}
