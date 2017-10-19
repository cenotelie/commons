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
 * Represents a couple of two elements
 *
 * @param <X> Type of the first element
 * @param <Y> Type of the second element
 * @author Laurent Wouters
 */
public class Couple<X, Y> {
    /**
     * The first element
     */
    public X x;
    /**
     * The second element
     */
    public Y y;

    /**
     * Initializes this couple
     */
    public Couple() {

    }

    /**
     * Initializes this couple
     *
     * @param x The first element
     * @param y The second element
     */
    public Couple(X x, Y y) {
        this.x = x;
        this.y = y;
    }
}
