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

package fr.cenotelie.commons.storage;

/**
 * Common constants for this package
 *
 * @author Laurent Wouters
 */
public interface Constants {
    /**
     * The number of bits to use in order to represent an index within a page
     */
    int PAGE_INDEX_LENGTH = 13;
    /**
     * The size of a page in bytes
     */
    int PAGE_SIZE = 1 << PAGE_INDEX_LENGTH;
    /**
     * The mask for the index within a page
     */
    long INDEX_MASK_LOWER = PAGE_SIZE - 1;
    /**
     * The mask for the index of a page
     */
    long INDEX_MASK_UPPER = ~INDEX_MASK_LOWER;
    /**
     * The null entry key, denotes the absence of value for a key
     */
    long KEY_NULL = 0xFFFFFFFFFFFFFFFFL;
}
