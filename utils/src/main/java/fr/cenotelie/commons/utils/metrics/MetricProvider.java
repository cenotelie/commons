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

import java.util.Collection;

/**
 * Represents an entity that can provides values for a metric
 *
 * @author Laurent Wouters
 */
public interface MetricProvider {
    /**
     * Gets the metrics provided by this provider
     *
     * @return The provided metrics
     */
    Collection<Metric> getMetrics();

    /**
     * Gets the last value for the specified metric
     *
     * @param metric The requested metric
     * @return The last value (or null if the metric is not provided)
     */
    MetricSnapshot pollMetric(Metric metric);
}
