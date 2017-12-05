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
import fr.cenotelie.commons.utils.api.*;
import fr.cenotelie.commons.utils.json.JsonDeserializer;

import java.util.List;

/**
 * Implements a proxy endpoint for sending requests to
 * the listening part of another local endpoint object in the current Java process
 *
 * @author Laurent Wouters
 */
public class LspEndpointRemoteProxy extends JsonRpcClientBase implements LspEndpointRemote {
    /**
     * The handler of the local endpoint to proxy
     */
    private final LspHandler handler;

    /**
     * Initializes this endpoint
     *
     * @param target The target endpoint
     */
    public LspEndpointRemoteProxy(LspEndpointLocal target) {
        this(target, null);
    }

    /**
     * Initializes this endpoint
     *
     * @param target       The target endpoint
     * @param deserializer The de-serializer to use for responses
     */
    public LspEndpointRemoteProxy(LspEndpointLocal target, JsonDeserializer deserializer) {
        super(deserializer);
        this.handler = target.getHandler();
    }

    @Override
    public Reply send(String message, JsonRpcContext context) {
        String response = handler.handle(message);
        if (response == null)
            return ReplySuccess.instance();
        return new ReplyResult<>(response);
    }

    @Override
    public Reply send(JsonRpcRequest request) {
        return new ReplyResult<>(handler.handle(request));
    }

    @Override
    public Reply send(List<JsonRpcRequest> requests) {
        return new ReplyResultCollection<>(handler.handle(requests));
    }

    @Override
    public Reply sendAndDeserialize(String message, JsonRpcContext context) {
        if (deserializer == null)
            return ReplyUnsupported.instance();
        String content = handler.handle(LspUtils.envelop(message));
        if (content == null)
            return ReplySuccess.instance();
        return deserializeResponses(LspUtils.stripEnvelope(content), context);
    }

    @Override
    public void close() {
        // do nothing
    }
}
