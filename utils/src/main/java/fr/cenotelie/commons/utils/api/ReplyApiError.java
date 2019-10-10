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
import fr.cenotelie.hime.redist.ASTNode;

/**
 * Implements a reply to a request when the request failed with an API error
 *
 * @author Laurent Wouters
 */
public class ReplyApiError implements Reply {
    /**
     * The API error to report
     */
    private final ApiError error;
    /**
     * The supplementary message for this error
     */
    private final String message;

    /**
     * Initializes this reply
     *
     * @param error The API error to report
     */
    public ReplyApiError(ApiError error) {
        this.error = error;
        this.message = null;
    }

    /**
     * Initializes this reply
     *
     * @param error   The API error to report
     * @param message The supplementary message for this error
     */
    public ReplyApiError(ApiError error, String message) {
        this.error = error;
        this.message = message;
    }

    /**
     * Loads an API error from its AST definition
     *
     * @param root The root of the AST definition
     * @return The API error
     */
    public static ApiError parseApiError(ASTNode root) {
        int code = 0;
        String message = "";
        String helpLink = "";
        for (ASTNode child : root.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            switch (name) {
                case "code": {
                    ASTNode nodeValue = child.getChildren().get(1);
                    String value = TextUtils.unescape(nodeValue.getValue());
                    code = Integer.parseInt(value);
                    break;
                }
                case "message": {
                    ASTNode nodeValue = child.getChildren().get(1);
                    message = TextUtils.unescape(nodeValue.getValue());
                    message = message.substring(1, message.length() - 1);
                    break;
                }
                case "helpLink": {
                    ASTNode nodeValue = child.getChildren().get(1);
                    helpLink = TextUtils.unescape(nodeValue.getValue());
                    helpLink = helpLink.substring(1, helpLink.length() - 1);
                    break;
                }
            }
        }
        return new ApiError(code, message, helpLink);
    }

    /**
     * Loads the supplementary message from the AST definition, if any
     *
     * @param root The root of the AST definition
     * @return The supplementary message, if any
     */
    public static String parseSupplementary(ASTNode root) {
        for (ASTNode child : root.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            if ("content".equals(name)) {
                ASTNode nodeValue = child.getChildren().get(1);
                String content = TextUtils.unescape(nodeValue.getValue());
                content = content.substring(1, content.length() - 1);
                return content;
            }
        }
        return null;
    }

    /**
     * Gets the supplementary message for this error
     *
     * @return The supplementary message for this error
     */
    public String getSupplementaryMessage() {
        return message;
    }

    /**
     * Gets the API error to report
     *
     * @return The API error to report
     */
    public ApiError getError() {
        return error;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    @Override
    public String getMessage() {
        return error.getMessage();
    }

    @Override
    public String serializedString() {
        return "ERROR: " + error.getMessage();
    }

    @Override
    public String serializedJSON() {
        return "{\"type\": \"" +
                TextUtils.escapeStringJSON(Reply.class.getCanonicalName()) +
                "\", \"kind\": \"" +
                TextUtils.escapeStringJSON(ReplyApiError.class.getSimpleName()) +
                "\", \"isSuccess\": false, \"message\": \"" +
                TextUtils.escapeStringJSON(message != null ? message : "") +
                "\", \"payload\": " + error.serializedJSON() + "}";
    }
}
