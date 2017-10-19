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

import fr.cenotelie.commons.utils.Identifiable;
import fr.cenotelie.commons.utils.Serializable;
import fr.cenotelie.commons.utils.collections.Couple;

import java.util.Collection;

/**
 * Represents a metric for statistics on the platform
 *
 * @author Laurent Wouters
 */
public interface Metric extends Identifiable, Serializable {
    /**
     * Hints whether this is a composite metric (composed of other metrics)
     */
    String HINT_IS_COMPOSITE = "isComposite";
    /**
     * Hints whether values for this metric are numeric
     */
    String HINT_IS_NUMERIC = "isNumeric";
    /**
     * Hints at the expected minimum numerical value for the metric
     */
    String HINT_MIN_VALUE = "minValue";
    /**
     * Hints at the expected maximum numerical value for this metric
     */
    String HINT_MAX_VALUE = "maxValue";

    /**
     * Gets the unit for the values of this metric
     *
     * @return The unit for the values of this metric
     */
    String getUnit();

    /**
     * Gets the time-to-live of a snapshot of this metric in nano-seconds
     *
     * @return The TTL of a snapshot of this metric
     */
    long getSnapshotTimeToLive();

    /**
     * Gets the hints for this metric
     *
     * @return The hints for this metric
     */
    Collection<Couple<String, String>> getHints();
}
