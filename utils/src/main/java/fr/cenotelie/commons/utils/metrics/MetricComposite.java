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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Represents a composite metric (composed of other metrics)
 *
 * @author Laurent Wouters
 */
public class MetricComposite implements Metric {
    /**
     * The metric's unique identifier
     */
    private final String identifier;
    /**
     * The metric's human readable name
     */
    private final String name;
    /**
     * The time-to-live of a snapshot of this metric in nano-seconds
     */
    private final long snapshotTTL;
    /**
     * The hints for this metric
     */
    private final Collection<Couple<String, String>> hints;
    /**
     * The parts that compose this metric
     */
    private final Collection<Metric> parts;

    /**
     * Initializes this metric
     *
     * @param identifier  The metric's unique identifier
     * @param name        The metric's human readable name
     * @param snapshotTTL The time-to-live of a snapshot of this metric in nano-seconds
     * @param parts       The parts that compose this metric
     */
    public MetricComposite(String identifier, String name, long snapshotTTL, Metric... parts) {
        this.identifier = identifier;
        this.name = name;
        this.snapshotTTL = snapshotTTL;
        this.hints = Collections.singletonList(new Couple<>(HINT_IS_COMPOSITE, "true"));
        this.parts = (parts == null || parts.length == 0) ? new ArrayList<Metric>() : Arrays.asList(parts);
    }

    /**
     * Initializes this metric as a proxy for a other one
     *
     * @param original The original to proxy
     * @param name     The metric's human readable name
     */
    public MetricComposite(MetricComposite original, String name) {
        this.identifier = original.identifier;
        this.name = name;
        this.snapshotTTL = original.snapshotTTL;
        this.hints = new ArrayList<>(original.hints);
        this.parts = new ArrayList<>(original.parts);
    }

    /**
     * Adds a part to this composite metric
     *
     * @param metric The part to add
     */
    public void addPart(Metric metric) {
        this.parts.add(metric);
    }

    /**
     * Removes all the parts of this metric
     */
    public void clearParts() {
        parts.clear();
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getUnit() {
        return "composite";
    }

    @Override
    public long getSnapshotTimeToLive() {
        return snapshotTTL;
    }

    @Override
    public Collection<Couple<String, String>> getHints() {
        return hints;
    }

    @Override
    public String serializedString() {
        return identifier;
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"type\": \"");
        builder.append(TextUtils.escapeStringJSON(Metric.class.getCanonicalName()));
        builder.append("\", \"identifier\": \"");
        builder.append(TextUtils.escapeStringJSON(identifier));
        builder.append("\", \"name\": \"");
        builder.append(TextUtils.escapeStringJSON(name));
        builder.append("\", \"unit\": \"composite\", \"snapshotTTL\":\"");
        builder.append(TextUtils.escapeStringJSON(Long.toString(snapshotTTL)));
        builder.append("\", \"hints\": {\"");
        builder.append(TextUtils.escapeStringJSON(HINT_IS_COMPOSITE));
        builder.append("\": \"true\"}, \"parts\": [");
        boolean first = true;
        for (Metric part : parts) {
            if (!first)
                builder.append(", ");
            first = false;
            builder.append(part.serializedJSON());
        }
        builder.append("]}");
        return builder.toString();
    }
}
