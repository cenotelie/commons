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

import fr.cenotelie.commons.lsp.server.LspServer;
import fr.cenotelie.commons.lsp.server.LspServerListener;
import fr.cenotelie.commons.utils.logging.Logging;

import java.util.concurrent.CountDownLatch;

/**
 * An object that runs an LSP server for the current Java process
 *
 * @author Laurent Wouters
 */
public abstract class LspRunner {
    /**
     * Exit code on a nominal shutdown
     */
    protected static final int EXIT_CODE_NORMAL = 0;
    /**
     * Exit code when an improper shutdown
     */
    protected static final int EXIT_CODE_ERROR = 1;

    /**
     * The LSP server to run
     */
    protected final LspServer server;
    /**
     * The signalling object for the main thread
     */
    protected final CountDownLatch signal;
    /**
     * Whether the server should stop
     */
    protected volatile boolean shouldStop;
    /**
     * Whether the server has shutdown properly
     */
    protected volatile boolean hasShutdown;

    /**
     * Initializes this runner
     *
     * @param server The LSP server to run
     */
    public LspRunner(LspServer server) {
        this.server = server;
        this.signal = new CountDownLatch(1);
        this.shouldStop = false;
        this.hasShutdown = false;
    }

    /**
     * Executes this runner
     */
    public void run() {
        server.registerListener(new LspServerListener() {
            @Override
            public void onInitialize() {
                // do nothing
            }

            @Override
            public void onShutdown() {
                hasShutdown = true;
            }

            @Override
            public void onExit() {
                doSignalClose();
            }
        });
        // register hook for shutdown events
        Runtime.getRuntime().addShutdownHook(new Thread(this::doSignalClose, LspRunnerStdStreams.class.getCanonicalName() + ".shutdown"));

        doRun();
        doClose();

        if (hasShutdown)
            System.exit(EXIT_CODE_NORMAL);
        else
            System.exit(EXIT_CODE_ERROR);
    }

    /**
     * Effectively runs the server
     * When this method returns, the server closes and exits
     */
    protected abstract void doRun();

    /**
     * When the runner is closing
     */
    protected void onClose() {
        // do nothing
    }

    /**
     * Signals that this is time to close this runner
     */
    protected void doSignalClose() {
        shouldStop = true;
        signal.countDown();
    }

    /**
     * Closes this runner
     */
    private void doClose() {
        try {
            server.close();
        } catch (Exception exception) {
            Logging.get().error(exception);
        }
        onClose();
    }
}
