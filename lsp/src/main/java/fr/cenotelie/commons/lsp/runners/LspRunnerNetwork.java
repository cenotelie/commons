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

package fr.cenotelie.commons.lsp.runners;

import fr.cenotelie.commons.lsp.LspEndpointRemoteStream;
import fr.cenotelie.commons.lsp.server.LspServer;
import fr.cenotelie.commons.utils.logging.Logging;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Runs a single LSP server in this Java process and serves it over a single port
 *
 * @author Laurent Wouters
 */
public class LspRunnerNetwork extends LspRunner {
    /**
     * A stream to write the received and sent messages to
     */
    private final OutputStream debug;
    /**
     * The port to listen on for connections
     */
    private final int port;
    /**
     * The server socket to use
     */
    private ServerSocket serverSocket;
    /**
     * The target socket
     */
    private Socket targetSocket;

    /**
     * Initializes this runner
     *
     * @param server The LSP server to run
     * @param port   The port to listen on for connections
     */
    public LspRunnerNetwork(LspServer server, int port) {
        this(server, port, null);
    }

    /**
     * Initializes this runner
     *
     * @param server The LSP server to run
     * @param port   The port to listen on for connections
     * @param debug  A stream to write the received and sent messages to
     */
    public LspRunnerNetwork(LspServer server, int port, OutputStream debug) {
        super(server);
        this.port = port;
        this.debug = debug;
    }

    @Override
    protected void doRun() {
        // create the server socket
        try {
            serverSocket = new ServerSocket(port);
        } catch (Exception exception) {
            Logging.get().error(exception);
            return;
        }

        // listen on the socket
        try {
            targetSocket = serverSocket.accept();
        } catch (Exception exception) {
            Logging.get().error(exception);
            return;
        }

        // bind the target socket
        try {
            LspEndpointRemoteStream remote = new LspEndpointRemoteStream(server, targetSocket.getOutputStream(), targetSocket.getInputStream(), debug) {
                @Override
                protected void onListenerEnded() {
                    doSignalClose();
                }
            };
            server.setRemote(remote);
        } catch (IOException exception) {
            Logging.get().error(exception);
            return;
        }

        while (!shouldStop) {
            try {
                signal.await();
            } catch (InterruptedException exception) {
                break;
            }
        }
    }

    @Override
    protected void onClose() {
        if (targetSocket != null && !targetSocket.isClosed()) {
            try {
                targetSocket.close();
            } catch (Exception exception) {
                // do nothing
            }
        }
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (Exception exception) {
                // do nothing
            }
        }
    }
}
