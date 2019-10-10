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

import fr.cenotelie.commons.jsonrpc.JsonRpcRequest;
import fr.cenotelie.commons.jsonrpc.JsonRpcResponse;
import fr.cenotelie.commons.jsonrpc.JsonRpcResponseError;
import fr.cenotelie.commons.jsonrpc.JsonRpcResponseResult;
import fr.cenotelie.commons.lsp.LspHandlerBase;
import fr.cenotelie.commons.lsp.LspUtils;
import fr.cenotelie.commons.lsp.engine.Symbol;
import fr.cenotelie.commons.lsp.engine.Workspace;
import fr.cenotelie.commons.lsp.structures.*;
import fr.cenotelie.commons.utils.api.Reply;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Represents the part of a LSP server that handles requests from a client
 *
 * @author Laurent Wouters
 */
public class LspServerHandlerBase extends LspHandlerBase {
    /**
     * The workspace to use
     */
    protected final Workspace workspace;
    /**
     * The parent server
     */
    protected LspServer server;

    /**
     * Initializes this server
     *
     * @param workspace The workspace to use
     */
    public LspServerHandlerBase(Workspace workspace) {
        super(new LspServerRequestDeserializer());
        this.workspace = workspace;
    }

    /**
     * Gets the workspace used by this handler
     *
     * @return The workspace used by this handler
     */
    Workspace getWorkspace() {
        return workspace;
    }

    /**
     * Sets the parent server
     *
     * @param server The parent server
     */
    protected void setServer(LspServer server) {
        this.server = server;
        this.workspace.setLocal(server);
    }

    @Override
    public JsonRpcResponse handle(JsonRpcRequest request) {
        int state = server.getState();
        if (state < LspServer.STATE_READY) {
            if ("initialize".equals(request.getMethod()))
                return onInitialize(request);
            if ("exit".equals(request.getMethod()))
                return onExit(request);
            if (request.isNotification())
                return null;
            return new JsonRpcResponseError(
                    request.getIdentifier(),
                    LspUtils.ERROR_SERVER_NOT_INITIALIZED,
                    "Server is not initialized",
                    null);
        } else if (state >= LspServer.STATE_EXITING) {
            if (request.isNotification())
                return null;
            return new JsonRpcResponseError(
                    request.getIdentifier(),
                    LspUtils.ERROR_SERVER_HAS_EXITED,
                    "Server has exited",
                    null);
        } else if (state >= LspServer.STATE_SHUTTING_DOWN) {
            if ("exit".equals(request.getMethod()))
                return onExit(request);
            if (request.isNotification())
                return null;
            return new JsonRpcResponseError(
                    request.getIdentifier(),
                    LspUtils.ERROR_SERVER_SHUT_DOWN,
                    "Server has shut down",
                    null);
        }

        // here the server is ready
        switch (request.getMethod()) {
            case "initialize":
                return JsonRpcResponseError.newInvalidRequest(request.getIdentifier());
            case "initialized":
                return onInitialized(request);
            case "shutdown":
                return onShutdown(request);
            case "exit":
                return onExit(request);
            case "$/cancelRequest":
                return onCancelRequest(request);
            case "workspace/didChangeConfiguration":
                return onWorkspaceDidChangeConfiguration(request);
            case "workspace/didChangeWatchedFiles":
                return onWorkspaceDidChangeWatchedFiles(request);
            case "workspace/symbol":
                return onWorkspaceSymbol(request);
            case "workspace/executeCommand":
                return onWorkspaceExecuteCommand(request);
            case "textDocument/didOpen":
                return onTextDocumentDidOpen(request);
            case "textDocument/didChange":
                return onTextDocumentDidChange(request);
            case "textDocument/willSave":
                return onTextDocumentWillSave(request);
            case "textDocument/willSaveWaitUntil":
                return onTextDocumentWillSaveUntil(request);
            case "textDocument/didSave":
                return onTextDocumentDidSave(request);
            case "textDocument/didClose":
                return onTextDocumentDidClose(request);
            case "textDocument/completion":
                return onTextDocumentCompletion(request);
            case "completionItem/resolve":
                return onCompletionItemResolve(request);
            case "textDocument/hover":
                return onTextDocumentHover(request);
            case "textDocument/signatureHelp":
                return onTextDocumentSignatureHelp(request);
            case "textDocument/references":
                return onTextDocumentReferences(request);
            case "textDocument/documentHighlight":
                return onTextDocumentHighlights(request);
            case "textDocument/documentSymbol":
                return onTextDocumentSymbols(request);
            case "textDocument/formatting":
                return onTextDocumentFormatting(request);
            case "textDocument/rangeFormatting":
                return onTextDocumentRangeFormatting(request);
            case "textDocument/onTypeFormatting":
                return onTextDocumentOnTypeFormatting(request);
            case "textDocument/definition":
                return onTextDocumentDefinition(request);
            case "textDocument/codeAction":
                return onTextDocumentCodeAction(request);
            case "textDocument/codeLens":
                return onTextDocumentCodeLenses(request);
            case "codeLens/resolve":
                return onCodeLensResolve(request);
            case "textDocument/documentLink":
                return onTextDocumentLink(request);
            case "documentLink/resolve":
                return onDocumentLinkResolve(request);
            case "textDocument/rename":
                return onTextDocumentRename(request);
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
     * The initialize request is sent as the first request from the client to the server.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onInitialize(JsonRpcRequest request) {
        InitializeParams params = (InitializeParams) request.getParams();
        Reply reply = server.initialize(params);
        if (!reply.isSuccess())
            return JsonRpcResponseError.newInternalError(request.getIdentifier());
        workspace.onInitWorkspace(params.getRootUri(), params.getRootPath());
        return new JsonRpcResponseResult<>(request.getIdentifier(), new InitializationResult(server.getServerCapabilities()));
    }

    /**
     * The initialized notification is sent from the client to the server after the client received the result
     * of the initialize request but before the client is sending any other request or notification to the server.
     * The server can use the initialized notification for example to dynamically register capabilities.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onInitialized(JsonRpcRequest request) {
        return null;
    }

    /**
     * The shutdown request is sent from the client to the server.
     * It asks the server to shut down, but to not exit (otherwise the response might not be delivered correctly to the client).
     * There is a separate exit notification that asks the server to exit.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onShutdown(JsonRpcRequest request) {
        Reply reply = server.shutdown();
        if (!reply.isSuccess())
            return JsonRpcResponseError.newInternalError(request.getIdentifier());
        return new JsonRpcResponseResult<>(request.getIdentifier(), null);
    }

    /**
     * A notification to ask the server to exit its process.
     * The server should exit with success code 0 if the shutdown request has been received before; otherwise with error code 1.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onExit(JsonRpcRequest request) {
        server.exit();
        return null;
    }

    /**
     * The base protocol offers support for request cancellation.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onCancelRequest(JsonRpcRequest request) {
        // by default drop this notification
        return null;
    }

    /**
     * A notification sent from the client to the server to signal the change of configuration settings.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onWorkspaceDidChangeConfiguration(JsonRpcRequest request) {
        DidChangeConfigurationParams params = (DidChangeConfigurationParams) request.getParams();
        // by default drop this notification
        return null;
    }

    /**
     * The watched files notification is sent from the client to the server when the client detects changes to files watched by the language client.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onWorkspaceDidChangeWatchedFiles(JsonRpcRequest request) {
        DidChangeWatchedFilesParams params = (DidChangeWatchedFilesParams) request.getParams();
        workspace.onFileEvents(params.getChanges());
        return null;
    }

    /**
     * The workspace symbol request is sent from the client to the server to list project-wide symbols matching the query string.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onWorkspaceSymbol(JsonRpcRequest request) {
        WorkspaceSymbolParams params = (WorkspaceSymbolParams) request.getParams();
        Collection<SymbolInformation> data = workspace.getSymbols().search(params.getQuery());
        return new JsonRpcResponseResult<>(request.getIdentifier(), data);
    }

    /**
     * The workspace/executeCommand request is sent from the client to the server to trigger command execution on the server.
     * In most cases the server creates a WorkspaceEdit structure and applies the changes to the workspace using the request workspace/applyEdit which is sent from the server to the client.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onWorkspaceExecuteCommand(JsonRpcRequest request) {
        ExecuteCommandParams params = (ExecuteCommandParams) request.getParams();
        Object result = workspace.executeCommand(params);
        return new JsonRpcResponseResult<>(request.getIdentifier(), result);
    }

    /**
     * The document open notification is sent from the client to the server to signal newly opened text documents.
     * The document's truth is now managed by the client and the server must not try to read the document's truth using the document's uri.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentDidOpen(JsonRpcRequest request) {
        DidOpenTextDocumentParams params = (DidOpenTextDocumentParams) request.getParams();
        workspace.onDocumentOpen(params.getTextDocument());
        return null;
    }

    /**
     * The document change notification is sent from the client to the server to signal changes to a text document.
     * In 2.0 the shape of the params has changed to include proper version numbers and language ids.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentDidChange(JsonRpcRequest request) {
        DidChangeTextDocumentParams params = (DidChangeTextDocumentParams) request.getParams();
        workspace.onDocumentChange(params.getTextDocument(), params.getContentChanges());
        return null;
    }

    /**
     * The document will save notification is sent from the client to the server before the document is actually saved.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentWillSave(JsonRpcRequest request) {
        WillSaveTextDocumentParams params = (WillSaveTextDocumentParams) request.getParams();
        workspace.onDocumentWillSave(params.getTextDocument(), params.getReason());
        return null;
    }

    /**
     * The document will save request is sent from the client to the server before the document is actually saved.
     * The request can return an array of TextEdits which will be applied to the text document before it is saved.
     * Please note that clients might drop results if computing the text edits took too long or if a server constantly fails on this request.
     * This is done to keep the save fast and reliable.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentWillSaveUntil(JsonRpcRequest request) {
        WillSaveTextDocumentParams params = (WillSaveTextDocumentParams) request.getParams();
        TextEdit[] edits = workspace.onDocumentWillSaveUntil(params.getTextDocument(), params.getReason());
        return new JsonRpcResponseResult<>(request.getIdentifier(), edits != null ? edits : new TextEdit[0]);
    }

    /**
     * The document save notification is sent from the client to the server when the document was saved in the client.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentDidSave(JsonRpcRequest request) {
        DidSaveTextDocumentParams params = (DidSaveTextDocumentParams) request.getParams();
        workspace.onDocumentDidSave(params.getTextDocument(), params.getText());
        return null;
    }

    /**
     * The document close notification is sent from the client to the server when the document got closed in the client.
     * The document's truth now exists where the document's uri points to (e.g. if the document's uri is a file uri the truth now exists on disk).
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentDidClose(JsonRpcRequest request) {
        DidCloseTextDocumentParams params = (DidCloseTextDocumentParams) request.getParams();
        workspace.onDocumentDidClose(params.getTextDocument());
        return null;
    }

    /**
     * The Completion request is sent from the client to the server to compute completion items at a given cursor position.
     * Completion items are presented in the IntelliSense user interface.
     * If computing full completion items is expensive, servers can additionally provide a handler for the completion item resolve request ('completionItem/resolve').
     * This request is sent when a completion item is selected in the user interface.
     * A typical use case is for example:
     * The 'textDocument/completion' request doesn't fill in the documentation property for returned completion items since it is expensive to compute.
     * When the item is selected in the user interface then a 'completionItem/resolve' request is sent with the selected completion item as a param.
     * The returned completion item should have the documentation property filled in.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentCompletion(JsonRpcRequest request) {
        TextDocumentPositionParams params = (TextDocumentPositionParams) request.getParams();
        CompletionList result = workspace.getCompletion(params);
        return new JsonRpcResponseResult<>(request.getIdentifier(), result);
    }

    /**
     * The request is sent from the client to the server to resolve additional information for a given completion item.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onCompletionItemResolve(JsonRpcRequest request) {
        CompletionItem params = (CompletionItem) request.getParams();
        CompletionItem result = workspace.resolveCompletion(params);
        return new JsonRpcResponseResult<>(request.getIdentifier(), result);
    }

    /**
     * The hover request is sent from the client to the server to request hover information at a given text document position.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentHover(JsonRpcRequest request) {
        TextDocumentPositionParams params = (TextDocumentPositionParams) request.getParams();
        Hover result = workspace.getHover(params);
        if (result == null)
            result = new Hover(new MarkupContent(MarkupKind.PLAIN_TEXT, ""));
        return new JsonRpcResponseResult<>(request.getIdentifier(), result);
    }

    /**
     * The signature help request is sent from the client to the server to request signature information at a given cursor position.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentSignatureHelp(JsonRpcRequest request) {
        TextDocumentPositionParams params = (TextDocumentPositionParams) request.getParams();
        SignatureHelp result = workspace.getSignatures(params);
        return new JsonRpcResponseResult<>(request.getIdentifier(), result);
    }

    /**
     * The references request is sent from the client to the server to resolve project-wide references for the symbol denoted by the given text document position.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentReferences(JsonRpcRequest request) {
        ReferenceParams params = (ReferenceParams) request.getParams();
        Symbol symbol = workspace.getSymbols().getSymbolAt(params.getTextDocument().getUri(), params.getPosition());
        if (symbol == null)
            return new JsonRpcResponseResult<>(request.getIdentifier(), new Object[0]);
        Collection<Location> result = symbol.getReferences();
        if (params.getContext().includeDeclaration())
            result.addAll(symbol.getDefinitions());
        return new JsonRpcResponseResult<>(request.getIdentifier(), result);
    }

    /**
     * The document highlight request is sent from the client to the server to resolve a document highlights for a given text document position.
     * For programming languages this usually highlights all references to the symbol scoped to this file.
     * However we kept 'textDocument/documentHighlight' and 'textDocument/references' separate requests since the first one is allowed to be more fuzzy.
     * Symbol matches usually have a DocumentHighlightKind of Read or Write whereas fuzzy or textual matches use Text as the kind.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentHighlights(JsonRpcRequest request) {
        TextDocumentPositionParams params = (TextDocumentPositionParams) request.getParams();
        Symbol symbol = workspace.getSymbols().getSymbolAt(params.getTextDocument().getUri(), params.getPosition());
        if (symbol == null)
            return new JsonRpcResponseResult<>(request.getIdentifier(), new Object[0]);
        Collection<DocumentHighlight> result = new ArrayList<>();
        Collection<Range> ranges = symbol.getDefinitionsIn(params.getTextDocument().getUri());
        if (ranges != null) {
            for (Range range : ranges) {
                result.add(new DocumentHighlight(range));
            }
        }
        ranges = symbol.getReferencesIn(params.getTextDocument().getUri());
        if (ranges != null) {
            for (Range range : ranges) {
                result.add(new DocumentHighlight(range));
            }
        }
        return new JsonRpcResponseResult<>(request.getIdentifier(), result);
    }

    /**
     * The document symbol request is sent from the client to the server to list all symbols found in a given text document.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentSymbols(JsonRpcRequest request) {
        DocumentSymbolParams params = (DocumentSymbolParams) request.getParams();
        Collection<SymbolInformation> data = workspace.getSymbols().getDefinitionsIn(params.getTextDocument().getUri());
        return new JsonRpcResponseResult<>(request.getIdentifier(), data);
    }

    /**
     * The document formatting request is sent from the server to the client to format a whole document.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentFormatting(JsonRpcRequest request) {
        DocumentFormattingParams params = (DocumentFormattingParams) request.getParams();
        TextEdit[] result = workspace.formatDocument(params);
        return new JsonRpcResponseResult<>(request.getIdentifier(), result);
    }

    /**
     * The document range formatting request is sent from the client to the server to format a given range in a document.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentRangeFormatting(JsonRpcRequest request) {
        DocumentRangeFormattingParams params = (DocumentRangeFormattingParams) request.getParams();
        TextEdit[] result = workspace.formatRange(params);
        return new JsonRpcResponseResult<>(request.getIdentifier(), result);
    }

    /**
     * The document on type formatting request is sent from the client to the server to format parts of the document during typing.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentOnTypeFormatting(JsonRpcRequest request) {
        DocumentOnTypeFormattingParams params = (DocumentOnTypeFormattingParams) request.getParams();
        TextEdit[] result = workspace.formatOnTyped(params);
        return new JsonRpcResponseResult<>(request.getIdentifier(), result);
    }

    /**
     * The goto definition request is sent from the client to the server to resolve the definition location of a symbol at a given text document position.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentDefinition(JsonRpcRequest request) {
        TextDocumentPositionParams params = (TextDocumentPositionParams) request.getParams();
        Symbol symbol = workspace.getSymbols().getSymbolAt(params.getTextDocument().getUri(), params.getPosition());
        if (symbol == null)
            return new JsonRpcResponseResult<>(request.getIdentifier(), new Object[0]);
        return new JsonRpcResponseResult<>(request.getIdentifier(), symbol.getDefinitions());
    }

    /**
     * The code action request is sent from the client to the server to compute commands for a given text document and range.
     * These commands are typically code fixes to either fix problems or to beautify/refactor code.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentCodeAction(JsonRpcRequest request) {
        CodeActionParams params = (CodeActionParams) request.getParams();
        Command[] result = workspace.getCodeActions(params);
        return new JsonRpcResponseResult<>(request.getIdentifier(), result);
    }

    /**
     * The code lens request is sent from the client to the server to compute code lenses for a given text document.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentCodeLenses(JsonRpcRequest request) {
        CodeLensParams params = (CodeLensParams) request.getParams();
        CodeLens[] result = workspace.getCodeLens(params);
        return new JsonRpcResponseResult<>(request.getIdentifier(), result);
    }

    /**
     * The code lens resolve request is sent from the client to the server to resolve the command for a given code lens item.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onCodeLensResolve(JsonRpcRequest request) {
        CodeLens params = (CodeLens) request.getParams();
        CodeLens result = workspace.resolveCodeLens(params);
        return new JsonRpcResponseResult<>(request.getIdentifier(), result);
    }

    /**
     * The document links request is sent from the client to the server to request the location of links in a document.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentLink(JsonRpcRequest request) {
        DocumentLinkParams params = (DocumentLinkParams) request.getParams();
        DocumentLink[] result = workspace.getDocumentLinks(params);
        if (result == null)
            result = new DocumentLink[0];
        return new JsonRpcResponseResult<>(request.getIdentifier(), result);
    }

    /**
     * The document link resolve request is sent from the client to the server to resolve the target of a given document link.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onDocumentLinkResolve(JsonRpcRequest request) {
        DocumentLink params = (DocumentLink) request.getParams();
        DocumentLink result = workspace.resolveDocumentLink(params);
        return new JsonRpcResponseResult<>(request.getIdentifier(), result);
    }

    /**
     * The rename request is sent from the client to the server to perform a workspace-wide rename of a symbol.
     *
     * @param request The request
     * @return The response
     */
    protected JsonRpcResponse onTextDocumentRename(JsonRpcRequest request) {
        RenameParams params = (RenameParams) request.getParams();
        WorkspaceEdit result = workspace.renameSymbol(params);
        if (result == null)
            return JsonRpcResponseError.newInvalidParameters(request.getIdentifier());
        return new JsonRpcResponseResult<>(request.getIdentifier(), result);
    }
}
