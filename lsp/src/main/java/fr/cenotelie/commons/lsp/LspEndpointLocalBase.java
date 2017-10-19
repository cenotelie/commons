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

import fr.cenotelie.commons.jsonrpc.JsonRpcClientBase;
import fr.cenotelie.commons.jsonrpc.JsonRpcContext;
import fr.cenotelie.commons.jsonrpc.JsonRpcRequest;
import fr.cenotelie.commons.utils.api.Reply;
import fr.cenotelie.commons.utils.api.ReplyResult;
import fr.cenotelie.commons.utils.json.JsonDeserializer;

import java.util.List;

/**
 * Base implementation of an LSP endpoint that is local to the current Java process
 *
 * @author Laurent Wouters
 */
public abstract class LspEndpointLocalBase extends JsonRpcClientBase implements LspEndpointLocal {
    /**
     * The handler for the requests coming to this endpoint
     */
    protected final LspHandler handler;
    /**
     * The remote endpoint to connect to
     */
    protected LspEndpointRemote remote;

    /**
     * Initializes this endpoint
     *
     * @param handler      The handler for the requests coming to this endpoint
     * @param deserializer The de-serializer to use for responses
     */
    protected LspEndpointLocalBase(LspHandler handler, JsonDeserializer deserializer) {
        this(handler, deserializer, null);
    }

    /**
     * Initializes this endpoint
     *
     * @param handler      The handler for the requests coming to this endpoint
     * @param deserializer The de-serializer to use for responses
     * @param remote       The remote endpoint to connect to
     */
    protected LspEndpointLocalBase(LspHandler handler, JsonDeserializer deserializer, LspEndpointRemote remote) {
        super(deserializer);
        this.handler = handler;
        this.remote = remote;
    }

    /**
     * Gets the remote endpoint to connect to
     *
     * @return The remote endpoint to connect to
     */
    public LspEndpointRemote getRemote() {
        return remote;
    }

    /**
     * Sets the remote endpoint to connect to
     *
     * @param remote The remote endpoint to connect to
     */
    public void setRemote(LspEndpointRemote remote) {
        this.remote = remote;
    }

    @Override
    public void close() throws Exception {
        LspEndpointRemote temp = remote;
        remote = null;
        if (temp != null) {
            temp.close();
        }
    }

    @Override
    public LspHandler getHandler() {
        return handler;
    }

    @Override
    public JsonDeserializer getResponsesDeserializer() {
        return deserializer;
    }

    @Override
    public Reply send(String message, JsonRpcContext context) {
        return remote.send(message, context);
    }

    @Override
    public Reply send(JsonRpcRequest request) {
        return remote.send(request);
    }

    @Override
    public Reply send(List<JsonRpcRequest> requests) {
        return remote.send(requests);
    }

    @Override
    public Reply sendAndDeserialize(String message, JsonRpcContext context) {
        Reply reply = send(LspUtils.envelop(message), context);
        if (!reply.isSuccess())
            return reply;
        String content = LspUtils.stripEnvelope(((ReplyResult<String>) reply).getData());
        return deserializeResponses(content, context);
    }
}
