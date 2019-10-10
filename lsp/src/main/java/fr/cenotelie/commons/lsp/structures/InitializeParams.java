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

package fr.cenotelie.commons.lsp.structures;

import fr.cenotelie.commons.utils.Serializable;
import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.commons.utils.json.Json;
import fr.cenotelie.commons.utils.json.JsonDeserializer;
import fr.cenotelie.commons.utils.json.JsonLexer;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * Parameters for the initialize request
 *
 * @author Laurent Wouters
 */
public class InitializeParams implements Serializable {
    /**
     * The process Id of the parent process that started the server.
     * Is null (-1) if the process has not been started by another process.
     * If the parent process is not alive then the server should exit (see exit notification) its process.
     */
    private final int processId;
    /**
     * The rootPath of the workspace.
     * Is null if no folder is open.
     *
     * @deprecated in favour of rootUri.
     */
    private final String rootPath;
    /**
     * The rootUri of the workspace.
     * Is null if no folder is open.
     * If both `rootPath` and `rootUri` are set `rootUri` wins.
     */
    private final String rootUri;
    /**
     * User provided initialization options.
     */
    private final Object initializationOptions;
    /**
     * The capabilities provided by the client (editor or tool)
     */
    private final ClientCapabilities capabilities;
    /**
     * The initial trace setting. If omitted trace is disabled ('off').
     */
    private final String trace;

    /**
     * Gets the process Id of the parent process that started the server.
     *
     * @return The process Id of the parent process that started the server.
     */
    public int getProcessId() {
        return processId;
    }

    /**
     * Gets the rootPath of the workspace.
     *
     * @return The rootPath of the workspace.
     * @deprecated in favour of rootUri.
     */
    public String getRootPath() {
        return rootPath;
    }

    /**
     * Gets the rootUri of the workspace.
     *
     * @return The rootUri of the workspace.
     */
    public String getRootUri() {
        return rootUri;
    }

    /**
     * Gets the user provided initialization options.
     *
     * @return The user provided initialization options.
     */
    public Object getInitializationOptions() {
        return initializationOptions;
    }

    /**
     * Gets the capabilities provided by the client (editor or tool)
     *
     * @return The capabilities provided by the client (editor or tool)
     */
    public ClientCapabilities getCapabilities() {
        return capabilities;
    }

    /**
     * Gets the initial trace setting
     *
     * @return The initial trace setting
     */
    public String getTrace() {
        return trace;
    }

    /**
     * Initializes this structure
     *
     * @param processId             The process Id of the parent process that started the server.
     * @param rootPath              The rootPath of the workspace.
     * @param rootUri               The rootUri of the workspace.
     * @param initializationOptions The user provided initialization options.
     * @param capabilities          The capabilities provided by the client (editor or tool)
     * @param trace                 The initial trace setting
     */
    public InitializeParams(
            int processId,
            String rootPath,
            String rootUri,
            Object initializationOptions,
            ClientCapabilities capabilities,
            String trace) {
        this.processId = processId;
        this.rootPath = rootPath;
        this.rootUri = rootUri;
        this.initializationOptions = initializationOptions;
        this.capabilities = capabilities;
        this.trace = trace;
    }

    /**
     * Initializes this structure
     *
     * @param definition   The serialized definition
     * @param deserializer The current deserializer
     */
    public InitializeParams(ASTNode definition, JsonDeserializer deserializer) {
        int processId = -1;
        String rootPath = null;
        String rootUri = null;
        Object initializationOptions = null;
        ClientCapabilities capabilities = null;
        String trace = "off";
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            ASTNode nodeValue = child.getChildren().get(1);
            switch (name) {
                case "processId": {
                    if (nodeValue.getSymbol().getID() == JsonLexer.ID.LITERAL_INTEGER)
                        processId = Integer.parseInt(nodeValue.getValue());
                    break;
                }
                case "rootPath": {
                    rootPath = TextUtils.unescape(nodeValue.getValue());
                    rootPath = rootPath.substring(1, rootPath.length() - 1);
                    break;
                }
                case "rootUri": {
                    rootUri = TextUtils.unescape(nodeValue.getValue());
                    rootUri = rootUri.substring(1, rootUri.length() - 1);
                    break;
                }
                case "initializationOptions": {
                    initializationOptions = deserializer.deserialize(nodeValue, this);
                    break;
                }
                case "capabilities": {
                    capabilities = new ClientCapabilities(nodeValue, deserializer);
                    break;
                }
                case "trace": {
                    trace = TextUtils.unescape(nodeValue.getValue());
                    trace = trace.substring(1, trace.length() - 1);
                    break;
                }
            }
        }
        this.processId = processId;
        this.rootPath = rootPath;
        this.rootUri = rootUri != null ? rootUri : "";
        this.initializationOptions = initializationOptions;
        this.capabilities = capabilities != null ? capabilities : new ClientCapabilities();
        this.trace = trace;
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"processId\": ");
        builder.append(processId);
        if (rootPath != null) {
            builder.append(", \"rootPath\": \"");
            builder.append(TextUtils.escapeStringJSON(rootPath));
            builder.append("\"");
        }
        builder.append(", \"rootUri\": \"");
        builder.append(TextUtils.escapeStringJSON(rootUri));
        builder.append("\"");
        if (initializationOptions != null) {
            builder.append(", \"initializationOptions\": ");
            Json.serialize(builder, initializationOptions);
        }
        builder.append(", \"capabilities\": ");
        Json.serialize(builder, capabilities);
        if (trace != null) {
            builder.append(", \"trace\": \"");
            builder.append(TextUtils.escapeStringJSON(trace));
            builder.append("\"");
        }
        builder.append("}");
        return builder.toString();
    }
}
