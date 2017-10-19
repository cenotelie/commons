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

package fr.cenotelie.commons.lsp.client;

import fr.cenotelie.commons.jsonrpc.JsonRpcRequest;
import fr.cenotelie.commons.jsonrpc.JsonRpcResponse;
import fr.cenotelie.commons.jsonrpc.JsonRpcResponseError;
import fr.cenotelie.commons.lsp.LspHandlerBase;

/**
 * Represents the part of a LSP client that handles requests from a server
 *
 * @author Laurent Wouters
 */
public abstract class LspClientHandlerBase extends LspHandlerBase {
    /**
     * Initializes this server
     */
    public LspClientHandlerBase() {
        super(new LspClientRequestDeserializer());
    }

    @Override
    public JsonRpcResponse handle(JsonRpcRequest request) {
        switch (request.getMethod()) {
            case "window/showMessage":
                return onWindowShowMessage(request);
            case "window/showMessageRequest":
                return onWindowShowMessageRequest(request);
            case "window/logMessage":
                return onWindowLogMessage(request);
            case "telemetry/event":
                return onTelemetryEvent(request);
            case "client/registerCapability":
                return onClientRegisterCapability(request);
            case "client/unregisterCapability":
                return onClientUnregisterCapability(request);
            case "workspace/applyEdit":
                return onWorkspaceApplyEdit(request);
            case "textDocument/publishDiagnostics":
                return onTextDocumentPublishDiagnostics(request);
            default:
                return onOther(request);
        }
    }

    /**
     * Responds to any other request
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onOther(JsonRpcRequest request) {
        return JsonRpcResponseError.newInvalidRequest(request.getIdentifier());
    }

    /**
     * The show message notification is sent from a server to a client to ask the client to display a particular message in the user interface.
     *
     * @param request The request
     * @return The response
     */
    protected abstract JsonRpcResponse onWindowShowMessage(JsonRpcRequest request);

    /**
     * The show message request is sent from a server to a client to ask the client to display a particular message in the user interface.
     * In addition to the show message notification the request allows to pass actions and to wait for an answer from the client.
     *
     * @param request The request
     * @return The response
     */
    protected abstract JsonRpcResponse onWindowShowMessageRequest(JsonRpcRequest request);

    /**
     * The log message notification is sent from the server to the client to ask the client to log a particular message.
     *
     * @param request The request
     * @return The response
     */
    protected abstract JsonRpcResponse onWindowLogMessage(JsonRpcRequest request);

    /**
     * The telemetry notification is sent from the server to the client to ask the client to log a telemetry event.
     *
     * @param request The request
     * @return The response
     */
    protected abstract JsonRpcResponse onTelemetryEvent(JsonRpcRequest request);

    /**
     * The client/registerCapability request is sent from the server to the client to register for a new capability on the client side.
     * Not all clients need to support dynamic capability registration.
     * A client opts in via the ClientCapabilities.dynamicRegistration property.
     *
     * @param request The request
     * @return The response
     */
    protected abstract JsonRpcResponse onClientRegisterCapability(JsonRpcRequest request);

    /**
     * The client/unregisterCapability request is sent from the server to the client to unregister a previously registered capability.
     *
     * @param request The request
     * @return The response
     */
    protected abstract JsonRpcResponse onClientUnregisterCapability(JsonRpcRequest request);

    /**
     * The workspace/applyEdit request is sent from the server to the client to modify resource on the client side.
     *
     * @param request The request
     * @return The response
     */
    protected abstract JsonRpcResponse onWorkspaceApplyEdit(JsonRpcRequest request);

    /**
     * Diagnostics notification are sent from the server to the client to signal results of validation runs.
     *
     * @param request The request
     * @return The response
     */
    protected abstract JsonRpcResponse onTextDocumentPublishDiagnostics(JsonRpcRequest request);
}
