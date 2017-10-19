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

package fr.cenotelie.commons.utils.metrics;

import fr.cenotelie.commons.utils.Serializable;

/**
 * Represents the snapshot value of a metric at a given time
 *
 * @param <T> The type of the metric's value
 * @author Laurent wouters
 */
public interface MetricSnapshot<T> extends Serializable {
    /**
     * Gets the timestamp and the time of this snapshot
     *
     * @return The timestamp and the time of this snapshot
     */
    long getTimestamp();

    /**
     * Gets the value for this snapshot
     *
     * @return The value
     */
    T getValue();
}
