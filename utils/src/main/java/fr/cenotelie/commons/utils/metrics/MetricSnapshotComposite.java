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

import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.commons.utils.collections.Couple;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Implements a snapshot for a composite metric
 *
 * @author Laurent Wouters
 */
public class MetricSnapshotComposite implements MetricSnapshot<Object> {
    /**
     * The timestamp for this snapshot
     */
    private final long timestamp;
    /**
     * The parts for this snapshot
     */
    private final Collection<Couple<Metric, MetricSnapshot>> parts;

    /**
     * Initializes this snapshot
     */
    public MetricSnapshotComposite() {
        this.timestamp = System.nanoTime();
        this.parts = new ArrayList<>();
    }

    /**
     * Initializes this snapshot
     *
     * @param timestamp The timestamp for this snapshot
     */
    public MetricSnapshotComposite(long timestamp) {
        this.timestamp = timestamp;
        this.parts = new ArrayList<>();
    }

    /**
     * Adds a part to this snapshot
     *
     * @param part     The part metric
     * @param snapshot The associated snapshot
     */
    public void addPart(Metric part, MetricSnapshot snapshot) {
        this.parts.add(new Couple<>(part, snapshot));
    }

    /**
     * Removes all the parts of this metric
     */
    public void clearParts() {
        parts.clear();
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public Object getValue() {
        return parts;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder("{\"type\": \"");
        builder.append(TextUtils.escapeStringJSON(MetricSnapshot.class.getCanonicalName()));
        builder.append("\", \"parts\": {");
        boolean first = true;
        for (Couple<Metric, MetricSnapshot> part : parts) {
            if (!first)
                builder.append(", ");
            first = false;
            builder.append("\"");
            builder.append(TextUtils.escapeStringJSON(part.x.getIdentifier()));
            builder.append("\": ");
            builder.append(part.y.serializedJSON());
        }

        builder.append("}}");
        return builder.toString();
    }
}
