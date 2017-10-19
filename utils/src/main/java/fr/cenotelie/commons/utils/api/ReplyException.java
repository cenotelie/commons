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

/**
 * Implements a reply to a request when the request failed due to internal exception
 *
 * @author Laurent Wouters
 */
public class ReplyException implements Reply {
    /**
     * The standard message for an exception
     */
    public static final String MESSAGE = "Internal server error, see log for more details.";

    /**
     * The thrown exception
     */
    private final Throwable throwable;

    /**
     * Initializes this reply
     *
     * @param throwable The thrown exception
     */
    public ReplyException(Throwable throwable) {
        this.throwable = throwable;
    }

    /**
     * Gets the thrown exception
     *
     * @return The thrown exception
     */
    public Throwable getThrowable() {
        return throwable;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    @Override
    public String getMessage() {
        if (throwable == null)
            return MESSAGE;
        String message = throwable.getMessage();
        return message != null ? message : MESSAGE;
    }

    @Override
    public String serializedString() {
        return "ERROR: " + getMessage();
    }

    @Override
    public String serializedJSON() {
        return "{\"type\": \"" +
                TextUtils.escapeStringJSON(Reply.class.getCanonicalName()) +
                "\", \"kind\": \"" +
                TextUtils.escapeStringJSON(ReplyException.class.getSimpleName()) +
                "\", \"isSuccess\": false," +
                "\"message\": \"" + TextUtils.escapeStringJSON(getMessage()) + "\"}";
    }
}
