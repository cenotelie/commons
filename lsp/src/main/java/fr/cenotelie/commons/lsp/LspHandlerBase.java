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

import fr.cenotelie.commons.jsonrpc.JsonRpcResponseError;
import fr.cenotelie.commons.jsonrpc.JsonRpcServerBase;
import fr.cenotelie.commons.utils.IOUtils;
import fr.cenotelie.commons.utils.json.Json;
import fr.cenotelie.commons.utils.json.JsonDeserializer;
import fr.cenotelie.commons.utils.logging.BufferedLogger;
import fr.cenotelie.commons.utils.logging.Logging;
import fr.cenotelie.hime.redist.ASTNode;

import java.io.IOException;
import java.io.Reader;

/**
 * Implements a basic handler of LSP requests
 *
 * @author Laurent Wouters
 */
public abstract class LspHandlerBase extends JsonRpcServerBase implements LspHandler {
    /**
     * Initializes this server
     *
     * @param deserializer The de-serializer to use for requests
     */
    public LspHandlerBase(JsonDeserializer deserializer) {
        super(deserializer);
    }

    @Override
    public JsonDeserializer getRequestsDeserializer() {
        return deserializer;
    }

    @Override
    public String handle(String input) {
        String content = LspUtils.stripEnvelope(input);
        if (content == null)
            return null;
        BufferedLogger logger = new BufferedLogger();
        ASTNode definition = Json.parse(logger, content);
        if (definition == null || !logger.getErrorMessages().isEmpty())
            return LspUtils.envelop(JsonRpcResponseError.newParseError(null).serializedJSON());
        String response = handle(definition);
        if (response == null)
            return null;
        return LspUtils.envelop(response);
    }

    @Override
    public String handle(Reader input) {
        try {
            return handle(IOUtils.read(input));
        } catch (IOException exception) {
            Logging.get().error(exception);
            return null;
        }
    }
}
