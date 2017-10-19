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

import java.io.OutputStream;

/**
 * Runs a single LSP server in this Java process and serves it with the standard input and output streams
 *
 * @author Laurent Wouters
 */
public class LspRunnerStdStreams extends LspRunner {
    /**
     * A stream to write the received and sent messages to
     */
    private final OutputStream debug;

    /**
     * Initializes this runner
     *
     * @param server The LSP server to run
     */
    public LspRunnerStdStreams(LspServer server) {
        this(server, null);
    }

    /**
     * Initializes this runner
     *
     * @param server The LSP server to run
     * @param debug  A stream to write the received and sent messages to
     */
    public LspRunnerStdStreams(LspServer server, OutputStream debug) {
        super(server);
        this.debug = debug;
    }

    @Override
    protected void doRun() {
        LspEndpointRemoteStream remote = new LspEndpointRemoteStream(server, System.out, System.in, debug) {
            @Override
            protected void onListenerEnded() {
                doSignalClose();
            }
        };
        server.setRemote(remote);

        while (!shouldStop) {
            try {
                signal.await();
            } catch (InterruptedException exception) {
                break;
            }
        }
    }
}
