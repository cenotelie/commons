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

import fr.cenotelie.commons.utils.Serializable;
import fr.cenotelie.commons.utils.TextUtils;

/**
 * Implements a successful reply to a request with an object of type T as a response
 *
 * @param <T> The type of return data
 * @author Laurent Wouters
 */
public class ReplyResult<T> implements Reply {
    /**
     * The payload
     */
    private final T data;

    /**
     * Initializes this result
     *
     * @param data The payload
     */
    public ReplyResult(T data) {
        this.data = data;
    }

    /**
     * Gets the payload
     *
     * @return The payload
     */
    public T getData() {
        return data;
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
        return data == null ? "NO DATA" : data.toString();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"type\": \"");
        builder.append(TextUtils.escapeStringJSON(Reply.class.getCanonicalName()));
        builder.append("\", \"kind\": \"");
        builder.append(TextUtils.escapeStringJSON(ReplyResult.class.getSimpleName()));
        builder.append("\", \"isSuccess\": true, \"message\": \"\", \"payload\": ");
        if (data == null)
            builder.append("\"\"");
        else if (data instanceof Serializable)
            builder.append(((Serializable) data).serializedJSON());
        else {
            builder.append("\"");
            builder.append(TextUtils.escapeStringJSON(data.toString()));
            builder.append("\"");
        }
        builder.append("}");
        return builder.toString();
    }
}
