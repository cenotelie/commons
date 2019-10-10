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
 * Base implementation of a metric
 *
 * @author Laurent Wouters
 */
public class MetricBase implements Metric {
    /**
     * The metric's unique identifier
     */
    private final String identifier;
    /**
     * The metric's human readable name
     */
    private final String name;
    /**
     * The unit for the values of this metric
     */
    private final String unit;
    /**
     * The time-to-live of a snapshot of this metric in nano-seconds
     */
    private final long snapshotTTL;
    /**
     * The hints for this metric
     */
    private final Collection<Couple<String, String>> hints;

    /**
     * Initializes this metric
     *
     * @param identifier  The metric's unique identifier
     * @param name        The metric's human readable name
     * @param unit        The unit for the values of this metric
     * @param snapshotTTL The time-to-live of a snapshot of this metric in nano-seconds
     * @param hints       The hints for this metric
     */
    @SafeVarargs
    public MetricBase(String identifier, String name, String unit, long snapshotTTL, Couple<String, String>... hints) {
        this.identifier = identifier;
        this.name = name;
        this.unit = unit;
        this.snapshotTTL = snapshotTTL;
        this.hints = (hints == null || hints.length == 0) ? Collections.emptyList() : Collections.unmodifiableCollection(Arrays.asList(hints));
    }

    /**
     * Initializes this metric as a proxy for a other one
     *
     * @param original The original to proxy
     * @param name     The metric's human readable name
     */
    public MetricBase(Metric original, String name) {
        this.identifier = original.getIdentifier();
        this.name = name;
        this.unit = original.getUnit();
        this.snapshotTTL = original.getSnapshotTimeToLive();
        this.hints = new ArrayList<>(original.getHints());
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
        return unit;
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
        builder.append("\", \"unit\": \"");
        builder.append(TextUtils.escapeStringJSON(unit));
        builder.append("\", \"snapshotTTL\": \"");
        builder.append(TextUtils.escapeStringJSON(Long.toString(snapshotTTL)));
        builder.append("\", \"hints\": {");
        boolean first = true;
        for (Couple<String, String> hint : hints) {
            if (!first)
                builder.append(", ");
            first = false;
            builder.append("\"");
            builder.append(TextUtils.escapeStringJSON(hint.x));
            builder.append("\": \"");
            builder.append(TextUtils.escapeStringJSON(hint.y));
            builder.append("\"");
        }
        builder.append("}}");
        return builder.toString();
    }
}
