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

package fr.cenotelie.commons.lsp.server;

import fr.cenotelie.commons.lsp.LspEndpointLocalBase;
import fr.cenotelie.commons.lsp.structures.ClientCapabilities;
import fr.cenotelie.commons.lsp.structures.InitializeParams;
import fr.cenotelie.commons.lsp.structures.ServerCapabilities;
import fr.cenotelie.commons.utils.api.Reply;
import fr.cenotelie.commons.utils.api.ReplyFailure;
import fr.cenotelie.commons.utils.api.ReplySuccess;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements a base LSP server
 *
 * @author Laurent Wouters
 */
public class LspServer extends LspEndpointLocalBase {
    /**
     * The server has been created, it has not been initialized yet
     */
    public static final int STATE_CREATED = 0;
    /**
     * The server is being initialized
     */
    public static final int STATE_INITIALIZING = 1;
    /**
     * The server is initialized and ready for work
     */
    public static final int STATE_READY = 2;
    /**
     * The server is shutting down
     */
    public static final int STATE_SHUTTING_DOWN = 3;
    /**
     * The server has been shut down
     */
    public static final int STATE_SHUT_DOWN = 4;
    /**
     * The server is exiting
     */
    public static final int STATE_EXITING = 5;
    /**
     * The server has exited
     */
    public static final int STATE_EXITED = 6;

    /**
     * The server's current state
     */
    private final AtomicInteger state;
    /**
     * The listeners for this server
     */
    private final Collection<LspServerListener> listeners;
    /**
     * The capabilities of this current server
     */
    protected final ServerCapabilities serverCapabilities;
    /**
     * The initialization parameters received from the client
     */
    protected InitializeParams clientInitializationParameters;

    /**
     * Initializes this endpoint
     *
     * @param handler The handler for the requests coming to this endpoint
     */
    public LspServer(LspServerHandlerBase handler) {
        super(handler, new LspServerResponseDeserializer());
        this.state = new AtomicInteger(STATE_CREATED);
        this.listeners = new ArrayList<>();
        this.serverCapabilities = handler.getWorkspace().getServerCapabilities();
        handler.setServer(this);
    }

    /**
     * Gets the server's current state
     *
     * @return The server's current state
     */
    public int getState() {
        return state.get();
    }

    /**
     * Register a listener
     *
     * @param listener A listener
     */
    public void registerListener(LspServerListener listener) {
        listeners.add(listener);
    }

    /**
     * Unregister a listener
     *
     * @param listener A listener
     */
    public void unregisterListener(LspServerListener listener) {
        listeners.remove(listener);
    }

    /**
     * Gets the server capabilities
     *
     * @return The server capabilities
     */
    public ServerCapabilities getServerCapabilities() {
        return serverCapabilities;
    }

    /**
     * Gets the capabilities of the connected client, if any
     *
     * @return The capabilities of the connected client, if any
     */
    public ClientCapabilities getClientCapabiltiies() {
        return clientInitializationParameters != null ? clientInitializationParameters.getCapabilities() : null;
    }

    /**
     * Performs the server's initialization
     *
     * @param params The initialization parameters
     * @return The reply
     */
    protected Reply initialize(InitializeParams params) {
        if (!state.compareAndSet(STATE_CREATED, STATE_INITIALIZING))
            return ReplyFailure.instance();
        clientInitializationParameters = params;
        Reply reply = doInitialize();
        if (!reply.isSuccess()) {
            state.compareAndSet(STATE_INITIALIZING, STATE_CREATED);
            return reply;
        }
        state.compareAndSet(STATE_INITIALIZING, STATE_READY);
        for (LspServerListener listener : listeners)
            listener.onInitialize();
        return reply;
    }

    /**
     * Do the server-specific work for its initialization
     *
     * @return The reply
     */
    protected Reply doInitialize() {
        return ReplySuccess.instance();
    }

    /**
     * Performs the server's shutdown
     *
     * @return The reply
     */
    protected Reply shutdown() {
        if (!state.compareAndSet(STATE_READY, STATE_SHUTTING_DOWN))
            return ReplyFailure.instance();
        Reply reply = doShutdown();
        if (!reply.isSuccess()) {
            state.compareAndSet(STATE_SHUTTING_DOWN, STATE_READY);
            return reply;
        }
        state.compareAndSet(STATE_SHUTTING_DOWN, STATE_SHUT_DOWN);
        for (LspServerListener listener : listeners)
            listener.onShutdown();
        return reply;
    }

    /**
     * Do the server-specific work for its shutdown
     *
     * @return The reply
     */
    protected Reply doShutdown() {
        return ReplySuccess.instance();
    }

    /**
     * Performs the server's exit
     *
     * @return The reply
     */
    protected Reply exit() {
        state.set(STATE_EXITING);
        Reply reply = doExit();
        state.set(STATE_EXITED);
        for (LspServerListener listener : listeners)
            listener.onExit();
        return reply;
    }

    /**
     * Do the server-specific work for its exit
     *
     * @return The reply
     */
    protected Reply doExit() {
        return ReplySuccess.instance();
    }
}