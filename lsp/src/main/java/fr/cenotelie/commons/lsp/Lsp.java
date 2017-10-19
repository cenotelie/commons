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

import fr.cenotelie.commons.lsp.client.LspClient;
import fr.cenotelie.commons.lsp.client.LspClientResponseDeserializer;
import fr.cenotelie.commons.lsp.server.LspServer;
import fr.cenotelie.commons.lsp.server.LspServerResponseDeserializer;

/**
 * Front API for the LSP protocol
 *
 * @author Laurent Wouters
 */
public class Lsp {
    /**
     * Connects a local client and a local server when they are both in the current Java process
     *
     * @param client The LSP client
     * @param server The LSP server
     */
    public static void connectLocal(LspClient client, LspServer server) {
        LspEndpointRemoteProxy proxyForClient = new LspEndpointRemoteProxy(server, new LspClientResponseDeserializer());
        LspEndpointRemoteProxy proxyForServer = new LspEndpointRemoteProxy(client, new LspServerResponseDeserializer());
        client.setRemote(proxyForClient);
        server.setRemote(proxyForServer);
    }

    /**
     * Connects a local client to a server running an remote process through the use of the standard input and output streams
     *
     * @param client  The local LSP client
     * @param process The process running the server to connect to
     * @return The created remote endpoint
     */
    public static LspEndpointRemote connectToProcess(LspClient client, Process process) {
        LspEndpointRemoteStream remote = new LspEndpointRemoteStream(
                client,
                process.getOutputStream(),
                process.getInputStream());
        client.setRemote(remote);
        return remote;
    }
}
