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
 * Implements a reply to a request when the user's session has expired
 *
 * @author Laurent Wouters
 */
public class ReplyExpiredSession implements Reply {
    /**
     * The singleton instance
     */
    private static ReplyExpiredSession INSTANCE = null;

    /**
     * Initializes this instance
     */
    private ReplyExpiredSession() {

    }

    /**
     * Gets the singleton instance
     *
     * @return The singleton instance
     */
    public synchronized static ReplyExpiredSession instance() {
        if (INSTANCE == null)
            INSTANCE = new ReplyExpiredSession();
        return INSTANCE;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    @Override
    public String getMessage() {
        return "EXPIRED";
    }

    @Override
    public String serializedString() {
        return "EXPIRED";
    }

    @Override
    public String serializedJSON() {
        return "{\"type\": \"" +
                TextUtils.escapeStringJSON(Reply.class.getCanonicalName()) +
                "\", \"kind\": \"" +
                TextUtils.escapeStringJSON(ReplyExpiredSession.class.getSimpleName()) +
                "\", \"isSuccess\": false," +
                "\"message\": \"EXPIRED\"}";
    }
}
