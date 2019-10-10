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
import fr.cenotelie.commons.utils.IOUtils;
import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.commons.utils.api.*;
import fr.cenotelie.commons.utils.json.Json;
import fr.cenotelie.commons.utils.json.JsonParser;
import fr.cenotelie.commons.utils.logging.BufferedLogger;
import fr.cenotelie.commons.utils.logging.Logging;
import fr.cenotelie.hime.redist.ASTNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implements a remote LSP endpoint that uses streams to communicate
 *
 * @author Laurent Wouters
 */
public class LspEndpointRemoteStream extends JsonRpcClientBase implements LspEndpointRemote {
    /**
     * Timeout (in ms) for waiting
     */
    private static final int TIMEOUT = 500;
    /**
     * The counter of threads
     */
    private static final AtomicInteger COUNTER = new AtomicInteger(0);
    /**
     * The bytes for the first header to read
     */
    private static final byte[] HEADER_LENGTH = LspUtils.HEADER_CONTENT_LENGTH.getBytes(IOUtils.UTF8);
    /**
     * The header-ending sequence to detect (\r\n\r\n)
     */
    private static final int[] HEADER_ENDING = new int[]{0x0D, 0x0A, 0x0D, 0x0A};

    /**
     * The local endpoint
     */
    private final LspEndpointLocal local;
    /**
     * The output stream for sending messages to the real remote endpoint
     */
    private final OutputStream output;
    /**
     * A stream to write the received and sent messages to
     */
    private final OutputStream debug;
    /**
     * The input stream to read messages from the real remote endpoint
     */
    private final InputStream input;
    /**
     * Listening thread
     */
    private final Thread thread;
    /**
     * Whether the listening thread must exit
     */
    private final AtomicBoolean mustExit;
    /**
     * The response waiting to be consumed
     */
    private final AtomicReference<String> response;
    /**
     * The barrier to use for waiting for a response
     */
    private final CyclicBarrier responseBarrier;
    /**
     * Whether a thread is waiting for a response
     */
    private final AtomicBoolean waitingResponses;

    /**
     * Initializes this remote endpoint
     *
     * @param local  the local endpoint
     * @param output The output stream for sending messages to the real remote endpoint
     * @param input  The input stream to read messages from the real remote endpoint
     */
    public LspEndpointRemoteStream(LspEndpointLocal local, OutputStream output, InputStream input) {
        this(local, output, input, null);
    }

    /**
     * Initializes this remote endpoint
     *
     * @param local  the local endpoint
     * @param output The output stream for sending messages to the real remote endpoint
     * @param input  The input stream to read messages from the real remote endpoint
     * @param debug  A stream to write the received and sent messages to
     */
    public LspEndpointRemoteStream(LspEndpointLocal local, OutputStream output, InputStream input, OutputStream debug) {
        super(local.getResponsesDeserializer());
        this.local = local;
        this.input = input;
        this.output = output;
        this.debug = debug;
        this.thread = new Thread(this::threadListen, LspEndpointRemoteStream.class.getCanonicalName() + ".Thread." + COUNTER.getAndIncrement());
        this.mustExit = new AtomicBoolean(false);
        this.thread.start();
        this.response = new AtomicReference<>(null);
        this.responseBarrier = new CyclicBarrier(2);
        this.waitingResponses = new AtomicBoolean(false);
    }

    @Override
    public synchronized Reply send(String message, JsonRpcContext context) {
        if (context.isEmpty())
            return sendNoReply(LspUtils.envelop(message));
        return sendAndGetReply(LspUtils.envelop(message));
    }

    /**
     * Prints a debug message on a the debug stream
     *
     * @param message The message to print
     */
    private void printDebug(String message) {
        if (debug == null)
            return;
        try {
            debug.write(TextUtils.escapeStringSpecials(message).getBytes(IOUtils.CHARSET));
            debug.write(IOUtils.LINE_SEPARATOR.getBytes(IOUtils.CHARSET));
            debug.flush();
        } catch (IOException exception) {
            // do nothing
        }
    }

    /**
     * Sends a message without expecting a reply
     *
     * @param message The message to send
     * @return The reply
     */
    private Reply sendNoReply(String message) {
        try {
            writeToOutput(message);
            return ReplySuccess.instance();
        } catch (IOException exception) {
            Logging.get().error(exception);
            return new ReplyException(exception);
        }
    }

    /**
     * Sends a message and get the reply
     *
     * @param message The message to send
     * @return The reply
     */
    private Reply sendAndGetReply(String message) {
        if (!waitingResponses.compareAndSet(false, true))
            return new ReplyFailure("Bad state");
        try {
            writeToOutput(message);
        } catch (IOException exception) {
            Logging.get().error(exception);
            waitingResponses.set(false);
            return new ReplyException(exception);
        }
        try {
            responseBarrier.await();
        } catch (Exception exception) {
            return new ReplyException(exception);
        }
        String content = response.getAndSet(null);
        waitingResponses.set(false);
        return new ReplyResult<>(content);
    }

    /**
     * Writes a payload to the output stream
     *
     * @param message The message to send
     * @throws IOException When writing failed
     */
    private synchronized void writeToOutput(String message) throws IOException {
        printDebug("<== " + message);
        byte[] bytes = message.getBytes(IOUtils.UTF8);
        output.write(bytes);
        output.flush();
    }

    @Override
    public void close() throws Exception {
        if (mustExit.compareAndSet(false, true)) {
            if (thread.isAlive()) {
                thread.interrupt();
                input.close();
                try {
                    thread.join(TIMEOUT);
                } catch (InterruptedException exception) {
                    Logging.get().error(exception);
                }
            }
        }
    }

    /**
     * Event when the listening thread ended
     */
    protected void onListenerEnded() {
        // do nothing
    }

    /**
     * Main method for the listening thread
     */
    private void threadListen() {
        try {
            while (!mustExit.get() && !thread.isInterrupted()) {
                try {
                    int length = threadReadHeaderLength();
                    if (length < 0)
                        return;
                    if (threadSkipUntilPayload() < 0)
                        return;
                    byte[] payload = threadReadPayload(length);
                    if (payload == null)
                        return;
                    String content = new String(payload, IOUtils.UTF8);
                    printDebug("==> " + content);
                    threadHandlePayload(content);
                } catch (Exception exception) {
                    // stream has been closed
                    if (debug != null) {
                        PrintWriter writer = new PrintWriter(debug);
                        exception.printStackTrace(writer);
                        try {
                            debug.flush();
                        } catch (IOException exception2) {
                            // do nothing
                        }
                    }
                    return;
                }
            }
        } catch (Throwable throwable) {
            Logging.get().error(throwable);
        } finally {
            onListenerEnded();
        }
    }

    /**
     * Reads the content-length header from the input stream
     *
     * @return The length of the payload, or -1 if the thread must exit
     * @throws IOException When reading the stream fails
     */
    private int threadReadHeaderLength() throws IOException {
        int index = 0;
        while (index < HEADER_LENGTH.length) {
            // expect to read the input at the current index
            if (mustExit.get() && thread.isInterrupted())
                return -1;
            int value = input.read();
            if (value < 0)
                return -1;
            if (value == (HEADER_LENGTH[index] & 0xFF))
                index++;
            else
                // garbage on the stream
                index = 0;
        }

        // we read the header name
        // read the colon
        int value = input.read();
        if (mustExit.get() && thread.isInterrupted() || value != 0x3A)
            // not the colon => exit
            return -1;

        // read the length
        while (true) {
            value = input.read();
            if (mustExit.get() && thread.isInterrupted() || value < 0)
                return -1;
            if (value >= 0x30 && value <= 0x39)
                // a digit
                break;
            if (value != 0x20)
                // not a space => error, exit
                return -1;
        }
        int length = value - 0x30;
        while (true) {
            value = input.read();
            if (mustExit.get() && thread.isInterrupted() || value < 0)
                return -1;
            if (value >= 0x30 && value <= 0x39) {
                length = length * 10 + (value - 0x30);
                continue;
            }
            if (value != 0x0D)
                // not the '\r'
                return -1;
            break;
        }
        value = input.read();
        if (mustExit.get() && thread.isInterrupted() || value != 0x0A)
            // not the '\n'
            return -1;
        return length;
    }

    /**
     * Skip all content until the header ending sequence is detected
     *
     * @return The result
     * @throws IOException When reading the stream fails
     */
    private int threadSkipUntilPayload() throws IOException {
        int index = 2;
        while (index < HEADER_ENDING.length) {
            // expect to read the input at the current index
            if (mustExit.get() && thread.isInterrupted())
                return -1;
            int value = input.read();
            if (value < 0)
                return -1;
            if (value == (HEADER_ENDING[index] & 0xFF))
                index++;
            else
                index = 0;
        }
        return 0;
    }

    /**
     * Reads a payload from the input
     *
     * @param length The length of the payload
     * @return The payload, or null if reading fails
     * @throws IOException When reading the stream fails
     */
    private byte[] threadReadPayload(int length) throws IOException {
        byte[] payload = new byte[length];
        int index = 0;
        while (index < length) {
            int read = input.read(payload, index, length - index);
            if (mustExit.get() && thread.isInterrupted() || read < 0)
                return null;
            index += read;
        }
        return payload;
    }

    /**
     * Handles the specified input payload
     *
     * @param payload An input payload
     * @return The result
     * @throws Exception When an error occurs
     */
    private int threadHandlePayload(String payload) throws Exception {
        BufferedLogger logger = new BufferedLogger();
        ASTNode definition = Json.parse(logger, payload);
        if (definition == null || !logger.getErrorMessages().isEmpty())
            // failed to parse the payload
            return threadTransmitUnknown(payload);
        // determine whether this is a request or a response
        int decision = threadInspectPayload(definition);
        if (decision < 0)
            return threadTransmitRequest(payload);
        if (decision > 0)
            return threadTransmitResponse(payload);
        return threadTransmitUnknown(payload);
    }

    /**
     * Inspects the specified payload to distinguish between a request and a response
     *
     * @param definition the payload definition
     * @return 0 for no decision, -1 for requests, 1 for responses
     */
    private int threadInspectPayload(ASTNode definition) {
        if (definition.getSymbol().getID() == JsonParser.ID.object) {
            return threadInspectPayloadObject(definition);
        } else if (definition.getSymbol().getID() == JsonParser.ID.array) {
            return threadInspectPayloadArray(definition);
        } else {
            return 0;
        }
    }

    /**
     * Inspects the specified payload to distinguish between a request and a response
     *
     * @param definition the payload definition
     * @return 0 for no decision, -1 for requests, 1 for responses
     */
    private int threadInspectPayloadObject(ASTNode definition) {
        for (ASTNode child : definition.getChildren()) {
            ASTNode nodeMemberName = child.getChildren().get(0);
            String name = TextUtils.unescape(nodeMemberName.getValue());
            name = name.substring(1, name.length() - 1);
            switch (name) {
                case "method":
                case "params":
                    return -1;
                case "result":
                case "error":
                    return 1;

            }
        }
        return 0;
    }

    /**
     * Inspects the specified payload to distinguish between a request and a response
     *
     * @param definition the payload definition
     * @return 0 for no decision, -1 for requests, 1 for responses
     */
    private int threadInspectPayloadArray(ASTNode definition) {
        for (ASTNode node : definition.getChildren()) {
            if (node.getSymbol().getID() == JsonParser.ID.object) {
                int decision = threadInspectPayloadObject(node);
                if (decision != 0)
                    return decision;
            }
        }
        return 0;
    }

    /**
     * Transmits an incoming message when the message could not be distinguished between a response and a request
     *
     * @param payload The payload to transmit
     * @return The result
     * @throws Exception When an error occurs
     */
    private int threadTransmitUnknown(String payload) throws Exception {
        if (responseBarrier.getParties() == 1)
            // a response is expected => maybe this is supposed to be the response
            return threadTransmitResponse(payload);
        else
            // this is supposed to be a request
            return threadTransmitRequest(payload);
    }

    /**
     * Transmits a response to a waiting thread
     *
     * @param payload The payload to transmit
     * @return The result
     * @throws Exception When an error occurs
     */
    private int threadTransmitResponse(String payload) throws Exception {
        if (responseBarrier.getParties() != 1)
            // no waiting thread
            throw new Exception("Invalid state");
        if (!response.compareAndSet(null, payload))
            throw new Exception("Invalid state");
        responseBarrier.await();
        return 0;
    }

    /**
     * Transmits a request to the local endpoint
     *
     * @param payload The request to transmit
     * @return The result
     * @throws Exception When an error occurs
     */
    private int threadTransmitRequest(String payload) throws Exception {
        String reply = local.getHandler().handle(LspUtils.envelop(payload));
        if (reply == null)
            return 0;
        writeToOutput(reply);
        return 0;
    }
}
