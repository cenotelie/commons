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

package fr.cenotelie.commons.lsp;

/**
 * Utility APIs and constants for LSP
 */
public class LspUtils {
    /**
     * The end of line string to use
     */
    public static final String EOL = "\r\n";
    /**
     * The MIME type for LSP messages
     */
    public static final String MIME_LSP = "application/vscode-jsonrpc";
    /**
     * The content of the Content-Type header
     */
    public static final String HEADER_CONTENT_TYPE_VALUE = MIME_LSP + "; charset=utf-8";
    /**
     * The name of the Content-Length header
     */
    public static final String HEADER_CONTENT_LENGTH = "Content-Length";
    /**
     * The name of the Content-Type header
     */
    public static final String HEADER_CONTENT_TYPE = "Content-Type";

    /**
     * The error code when the server is not initialized
     */
    public static final int ERROR_SERVER_NOT_INITIALIZED = -32002;
    /**
     * The error code when the server has shut down
     */
    public static final int ERROR_SERVER_SHUT_DOWN = -32003;
    /**
     * The error code when the server has exited (should not happen)
     */
    public static final int ERROR_SERVER_HAS_EXITED = -32004;

    /**
     * Gets the full message for the specified content
     *
     * @param content The content
     * @return The full message with the envelope
     */
    public static String envelop(String content) {
        if (content == null)
            return null;
        return HEADER_CONTENT_LENGTH + ": " + content.length() + EOL +
                HEADER_CONTENT_TYPE + ": " + HEADER_CONTENT_TYPE_VALUE + EOL +
                EOL +
                content;
    }

    /**
     * Gets the content Json-Rpc payload after the header
     *
     * @param message The input message
     * @return The content
     */
    public static String stripEnvelope(String message) {
        if (message == null)
            return null;
        if (!message.startsWith(HEADER_CONTENT_LENGTH))
            return null;
        int index = message.indexOf(EOL, HEADER_CONTENT_LENGTH.length() + 1);
        if (index == -1)
            return null;
        int length;
        try {
            length = Integer.parseInt(message.substring(HEADER_CONTENT_LENGTH.length() + 1, index).trim());
        } catch (NumberFormatException exception) {
            return null;
        }
        if (length < 0) {
            return null;
        }
        message = message.substring(index + EOL.length());
        if (message.startsWith(HEADER_CONTENT_TYPE)) {
            index = message.indexOf(EOL, HEADER_CONTENT_TYPE.length() + 1);
            if (index == -1)
                return null;
            message = message.substring(index + EOL.length());
        }
        if (message.startsWith(EOL))
            message = message.substring(EOL.length());
        return message;
    }
}
