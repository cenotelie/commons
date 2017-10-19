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

package fr.cenotelie.commons.utils.api;

import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.commons.utils.json.Json;

import java.util.Collection;
import java.util.Collections;

/**
 * Implements a successful reply to a request with a collection of objects of type T as a response
 *
 * @param <T> The type of return data
 * @author Laurent Wouters
 */
public class ReplyResultCollection<T> implements Reply {
    /**
     * The payload
     */
    private final Collection<T> data;

    /**
     * Gets the payload
     *
     * @return The payload
     */
    public Collection<T> getData() {
        return data;
    }

    /**
     * Initializes this result
     *
     * @param data The payload
     */
    public ReplyResultCollection(Collection<T> data) {
        this.data = Collections.unmodifiableCollection(data);
    }

    @Override
    public boolean isSuccess() {
        return true;
    }

    @Override
    public String getMessage() {
        return serializedString();
    }

    @Override
    public String serializedString() {
        StringBuilder builder = new StringBuilder("[");
        boolean first = true;
        for (T obj : data) {
            if (!first)
                builder.append(", ");
            first = false;
            builder.append(obj.toString());
        }
        builder.append("]");
        return builder.toString();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"type\": \"");
        builder.append(TextUtils.escapeStringJSON(Reply.class.getCanonicalName()));
        builder.append("\", \"kind\": \"");
        builder.append(TextUtils.escapeStringJSON(ReplyResultCollection.class.getSimpleName()));
        builder.append("\", \"isSuccess\": true, \"message\": \"\", \"payload\": [");
        boolean first = true;
        for (T obj : data) {
            if (!first)
                builder.append(", ");
            first = false;
            Json.serialize(builder, obj);
        }
        builder.append("]}");
        return builder.toString();
    }
}
