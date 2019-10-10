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
 * Represents an error that can be shown to an end-user
 *
 * @author Laurent Wouters
 */
public class ApiError implements Serializable {
    /**
     * The unique error code
     */
    private final int code;
    /**
     * The message for this error
     */
    private final String message;
    /**
     * A link to the help for this error
     */
    private final String helpLink;

    /**
     * Initializes this error
     *
     * @param code     The unique error code
     * @param message  The message for this error
     * @param helpLink A link to the help for this error
     */
    public ApiError(int code, String message, String helpLink) {
        this.code = code;
        this.message = message;
        this.helpLink = helpLink;
    }

    /**
     * Gets the unique error code
     *
     * @return The unique error code
     */
    public int getCode() {
        return code;
    }

    /**
     * Gets the message for this error
     *
     * @return The message for this error
     */
    public String getMessage() {
        return message;
    }

    /**
     * Gets a link to the help for this error
     *
     * @return A link to the help for this error
     */
    public String getHelpLink() {
        return helpLink;
    }

    @Override
    public String serializedString() {
        return code + " - " + message;
    }

    @Override
    public String serializedJSON() {
        return "{\"type\": \"" +
                TextUtils.escapeStringJSON(ApiError.class.getCanonicalName()) +
                "\", \"code\": " +
                code +
                ", \"message\": \"" +
                TextUtils.escapeStringJSON(message) +
                "\", \"helpLink\": \"" +
                TextUtils.escapeStringJSON(helpLink) +
                "\" }";
    }

    /**
     * Serializes this error with a supplementary message
     *
     * @param supplementary The supplementary message, if any
     * @return The serialized error
     */
    public String serializedJSON(String supplementary) {
        return "{\"type\": \"" +
                TextUtils.escapeStringJSON(ApiError.class.getCanonicalName()) +
                "\", \"code\": " +
                code +
                ", \"message\": \"" +
                TextUtils.escapeStringJSON(message) +
                "\", \"helpLink\": \"" +
                TextUtils.escapeStringJSON(helpLink) +
                "\", \"content\": \"" +
                TextUtils.escapeStringJSON(supplementary != null ? supplementary : "") +
                "\" }";
    }
}
