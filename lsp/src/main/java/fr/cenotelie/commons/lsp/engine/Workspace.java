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

package fr.cenotelie.commons.lsp.engine;

import fr.cenotelie.commons.jsonrpc.JsonRpcRequest;
import fr.cenotelie.commons.lsp.LspEndpointLocal;
import fr.cenotelie.commons.lsp.server.LspServer;
import fr.cenotelie.commons.lsp.structures.*;
import fr.cenotelie.commons.utils.IOUtils;
import fr.cenotelie.commons.utils.json.SerializedUnknown;
import fr.cenotelie.commons.utils.logging.Logging;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the current workspace for a server
 *
 * @author Laurent Wouters
 */
public class Workspace {
    /**
     * The documents in the workspace
     */
    protected final Map<String, Document> documents;
    /**
     * The global symbol registry
     */
    protected final SymbolRegistry symbolRegistry;
    /**
     * The local LSP endpoint
     */
    protected LspEndpointLocal local;

    /**
     * Initializes an empty workspace
     */
    public Workspace() {
        this.documents = new HashMap<>();
        this.symbolRegistry = new SymbolRegistry();
    }

    /**
     * Gets the server capabilities that are supported by this workspace
     *
     * @return The server capabilities that are supported by this workspace
     */
    public ServerCapabilities getServerCapabilities() {
        ServerCapabilities capabilities = new ServerCapabilities();
        capabilities.addCapability("textDocumentSync.openClose");
        capabilities.addCapability("textDocumentSync.willSave");
        capabilities.addCapability("textDocumentSync.willSaveWaitUntil");
        capabilities.addCapability("textDocumentSync.save.includeText");
        capabilities.addOption("textDocumentSync.change", TextDocumentSyncKind.INCREMENTAL);
        listServerCapabilities(capabilities);
        return capabilities;
    }

    /**
     * Gets the client capabilities that are supported by this workspace
     *
     * @return The client capabilities that are supported by this workspace
     */
    public ClientCapabilities getClientCapabilities() {
        ClientCapabilities capabilities = new ClientCapabilities();
        listClientCapabilities(capabilities);
        return capabilities;
    }

    /**
     * Gets the documents in the workspace
     *
     * @return The documents in the workspace
     */
    public Collection<Document> getDocuments() {
        return documents.values();
    }

    /**
     * Gets the document for the specified URI
     *
     * @param uri The URI of a document
     * @return The document, or null if it does not exist
     */
    public Document getDocument(String uri) {
        return documents.get(uri);
    }

    /**
     * Gets the associated symbol registry
     *
     * @return The associated symbol registry
     */
    public SymbolRegistry getSymbols() {
        return symbolRegistry;
    }

    /**
     * Gets the local LSP endpoint
     *
     * @return The local LSP endpoint
     */
    public LspEndpointLocal getLocal() {
        return local;
    }

    /**
     * Sets the local LSP endpoint
     *
     * @param local The local LSP endpoint
     */
    public void setLocal(LspEndpointLocal local) {
        this.local = local;
    }

    /**
     * Initializes this workspace
     *
     * @param rootUri  The root uri for the workspace
     * @param rootPath The root path for the workspace
     */
    public void onInitWorkspace(String rootUri, String rootPath) {
        File workspaceRoot = null;
        if (rootUri != null && rootUri.startsWith("file://"))
            workspaceRoot = new File(rootUri.substring("file://".length()));
        else if (rootPath != null)
            workspaceRoot = new File(rootPath);
        if (workspaceRoot != null && workspaceRoot.exists())
            scanWorkspace(workspaceRoot);
    }

    /**
     * Scans the specified content
     *
     * @param file The current file or directory
     */
    private void scanWorkspace(File file) {
        if (isWorkspaceExcluded(file))
            return;
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files == null)
                return;
            for (int i = 0; i != files.length; i++)
                scanWorkspace(files[i]);
        } else {
            if (!isWorkspaceIncluded(file))
                return;
            Document document = resolveDocument(file);
            if (document == null)
                return;
            doDocumentAnalysis(document, false);
        }
    }

    /**
     * Determines whether the specified file or directory is excluded
     *
     * @param file The current file or directory
     * @return true if the file is excluded
     */
    protected boolean isWorkspaceExcluded(File file) {
        if (file.isDirectory()) {
            String name = file.getName();
            return (name.equals(".git") || name.equals(".hg") || name.equals(".svn"));
        }
        return false;
    }

    /**
     * Determines whether the specified file should be analyzed
     *
     * @param file The current file
     * @return true if the file should be analyzed
     */
    protected boolean isWorkspaceIncluded(File file) {
        return false;
    }

    /**
     * Resolves the document for the specified file
     *
     * @param file The file
     * @return The document
     */
    protected Document resolveDocument(File file) {
        String uri = "file://" + file.getAbsolutePath();
        Document document = documents.get(uri);
        if (document == null) {
            try (Reader reader = IOUtils.getAutoReader(file)) {
                String content = IOUtils.read(reader);
                document = new Document(uri, getLanguageFor(file), 0, content);
                documents.put(uri, document);
            } catch (IOException exception) {
                Logging.get().error(exception);
            }
        }
        return document;
    }

    /**
     * Gets the language associated to the specified file
     *
     * @param file The file
     * @return The associated language
     */
    protected String getLanguageFor(File file) {
        return "text";
    }

    /**
     * When file events have been received
     *
     * @param events The received file events
     */
    public void onFileEvents(FileEvent[] events) {
        for (int i = 0; i != events.length; i++) {
            onFileEvent(events[i]);
        }
    }

    /**
     * When file event have been received
     *
     * @param event The received file event
     */
    private void onFileEvent(FileEvent event) {
        switch (event.getType()) {
            case FileChangeType.DELETED: {
                Document document = documents.get(event.getUri());
                if (document == null)
                    return;
                documents.remove(document.getUri());
                symbolRegistry.onDocumentRemoved(document);
                break;
            }
            case FileChangeType.CREATED: {
                if (!event.getUri().startsWith("file://"))
                    return;
                File file = new File(event.getUri().substring("file://".length()));
                if (!file.exists() || isWorkspaceExcluded(file) || !isWorkspaceIncluded(file))
                    return;
                Document document = resolveDocument(file);
                if (document == null)
                    return;
                doDocumentAnalysis(document, true);
                break;
            }
            case FileChangeType.CHANGED: {
                Document document = documents.get(event.getUri());
                if (document == null)
                    return;
                doDocumentAnalysis(document, true);
                break;
            }
        }
    }

    /**
     * When a text document has been open
     *
     * @param documentItem The document item
     */
    public void onDocumentOpen(TextDocumentItem documentItem) {
        Document document = new Document(
                documentItem.getUri(),
                documentItem.getLanguageId(),
                documentItem.getVersion(),
                documentItem.getText());
        documents.put(documentItem.getUri(), document);
        doDocumentAnalysis(document, true);
    }

    /**
     * When document changes occurred on the client
     *
     * @param textDocument   The document that did change
     * @param contentChanges The actual content changes
     */
    public void onDocumentChange(VersionedTextDocumentIdentifier textDocument, TextDocumentContentChangeEvent[] contentChanges) {
        Document document = documents.get(textDocument.getUri());
        if (document != null) {
            document.mutateTo(textDocument.getVersion(), contentChanges);
            doDocumentAnalysis(document, true);
        }
    }

    /**
     * When a document is being saved
     *
     * @param textDocument The document that was is being saved
     * @param reason       The reason for the save
     */
    public void onDocumentWillSave(TextDocumentIdentifier textDocument, int reason) {
        // do nothing
    }

    /**
     * When a document is being saved
     *
     * @param textDocument The document that was is being saved
     * @param reason       The reason for the save
     */
    public TextEdit[] onDocumentWillSaveUntil(TextDocumentIdentifier textDocument, int reason) {
        // do nothing
        return null;
    }

    /**
     * When a document has been saved on the client
     *
     * @param textDocument The document that was saved
     * @param text         The full text for the saved document, if available
     */
    public void onDocumentDidSave(TextDocumentIdentifier textDocument, String text) {
        if (text != null) {
            Document document = documents.get(textDocument.getUri());
            if (document != null) {
                document.setFullContent(text);
                doDocumentAnalysis(document, true);
            }
        }
    }

    /**
     * When a document has been closed on the client
     *
     * @param textDocument The document that was closed
     */
    public void onDocumentDidClose(TextDocumentIdentifier textDocument) {
        // do nothing
    }

    /**
     * Performs the analysis of a document
     *
     * @param document           The document
     * @param publishDiagnostics Whether to publish the diagnostics
     */
    protected void doDocumentAnalysis(Document document, boolean publishDiagnostics) {
        DocumentAnalyzer analyzer = getServiceAnalyzer(document);
        if (analyzer == null)
            return;
        DocumentAnalysis analysis = analyzer.analyze(symbolRegistry, document);
        document.setLastAnalysis(analysis);
        if (local != null && publishDiagnostics) {
            local.send(new JsonRpcRequest(
                    null,
                    "textDocument/publishDiagnostics",
                    new PublishDiagnosticsParams(document.getUri(), analysis.getDiagnostics().toArray(new Diagnostic[0]))
            ));
        }
        symbolRegistry.onDocumentChanged(document, analysis.getSymbols());
    }

    /**
     * Executes a command on this workspace
     *
     * @param parameters The parameters for this request
     * @return The result of the command execution
     */
    public Object executeCommand(ExecuteCommandParams parameters) {
        return null;
    }

    /**
     * Gets the completion items for the specified document and position
     *
     * @param parameters The text document parameters
     * @return The list of completion items
     */
    public CompletionList getCompletion(TextDocumentPositionParams parameters) {
        Document document = documents.get(parameters.getTextDocument().getUri());
        if (document == null)
            return new CompletionList(false, new CompletionItem[0]);
        DocumentCompleter completer = getServiceCompleter(document);
        if (completer == null)
            return new CompletionList(false, new CompletionItem[0]);
        CompletionList result = completer.getCompletionItems(document, parameters.getPosition());
        SerializedUnknown data = new SerializedUnknown();
        data.addProperty("documentUri", parameters.getTextDocument().getUri());
        data.addProperty("locationLine", parameters.getPosition().getLine());
        data.addProperty("locationChar", parameters.getPosition().getCharacter());
        for (CompletionItem item : result.getItems()) {
            item.setData(data);
        }
        return result;
    }

    /**
     * Resolves a completion item
     *
     * @param item The completion item to resolve
     * @return The resolved completion item
     */
    public CompletionItem resolveCompletion(CompletionItem item) {
        if (item.getData() == null)
            return item;
        SerializedUnknown data = (SerializedUnknown) item.getData();
        Document document = documents.get(data.getValueFor("documentUri"));
        if (document == null)
            return item;
        DocumentCompleter completer = getServiceCompleter(document);
        if (completer == null)
            return item;
        return completer.resolve(document, new Position((Integer) data.getValueFor("locationLine"), (Integer) data.getValueFor("locationChar")), item);
    }

    /**
     * Gets the hover data for the specified document and position
     *
     * @param parameters The text document parameters
     * @return The hover data
     */
    public Hover getHover(TextDocumentPositionParams parameters) {
        Document document = documents.get(parameters.getTextDocument().getUri());
        if (document == null)
            return new Hover(new MarkupContent(MarkupKind.PLAIN_TEXT, ""));
        DocumentHoverProvider service = getServiceHoverProvider(document);
        if (service == null)
            return new Hover(new MarkupContent(MarkupKind.PLAIN_TEXT, ""));
        return service.getHoverData(document, parameters.getPosition());
    }

    /**
     * Gets the signature help for the specified document and position
     *
     * @param parameters The text document parameters
     * @return The signature help
     */
    public SignatureHelp getSignatures(TextDocumentPositionParams parameters) {
        Document document = documents.get(parameters.getTextDocument().getUri());
        if (document == null)
            return new SignatureHelp(new SignatureInformation[0]);
        DocumentSignatureHelper service = getServiceSignatureHelp(document);
        if (service == null)
            return new SignatureHelp(new SignatureInformation[0]);
        return service.getSignatures(document, parameters.getPosition());
    }

    /**
     * Formats a whole document
     *
     * @param parameters The parameters for this request
     * @return The edits representing the formatting result
     */
    public TextEdit[] formatDocument(DocumentFormattingParams parameters) {
        Document document = documents.get(parameters.getTextDocument().getUri());
        if (document == null)
            return new TextEdit[0];
        DocumentFormatter service = getServiceFormatter(document);
        if (service == null)
            return new TextEdit[0];
        return service.format(parameters.getOptions(), document);
    }

    /**
     * Formats a range in document
     *
     * @param parameters The parameters for this request
     * @return The edits representing the formatting result
     */
    public TextEdit[] formatRange(DocumentRangeFormattingParams parameters) {
        Document document = documents.get(parameters.getTextDocument().getUri());
        if (document == null)
            return new TextEdit[0];
        DocumentFormatter service = getServiceFormatter(document);
        if (service == null)
            return new TextEdit[0];
        return service.format(parameters.getOptions(), document, parameters.getRange());
    }

    /**
     * Formats a part of the document when a character has been typed
     *
     * @param parameters The parameters for this request
     * @return The edits representing the formatting result
     */
    public TextEdit[] formatOnTyped(DocumentOnTypeFormattingParams parameters) {
        Document document = documents.get(parameters.getTextDocument().getUri());
        if (document == null)
            return new TextEdit[0];
        DocumentFormatter service = getServiceFormatter(document);
        if (service == null)
            return new TextEdit[0];
        return service.format(parameters.getOptions(), document, parameters.getPosition(), parameters.getCharacter());
    }

    /**
     * Gets available code actions for the specified parameters
     *
     * @param parameters The parameters for this request
     * @return The available code actions
     */
    public Command[] getCodeActions(CodeActionParams parameters) {
        Document document = documents.get(parameters.getTextDocument().getUri());
        if (document == null)
            return new Command[0];
        DocumentActionProvider service = getServiceActionProvider(document);
        if (service == null)
            return new Command[0];
        return service.getActions(document, parameters.getRange(), parameters.getContext());
    }

    /**
     * Gets the code lens for the specified parameters
     *
     * @param parameters The parameters for this request
     * @return The available code lens
     */
    public CodeLens[] getCodeLens(CodeLensParams parameters) {
        Document document = documents.get(parameters.getTextDocument().getUri());
        if (document == null)
            return new CodeLens[0];
        DocumentLensProvider service = getServiceLensProvider(document);
        if (service == null)
            return new CodeLens[0];
        CodeLens[] lenses = service.getLens(document);
        String data = parameters.getTextDocument().getUri();
        for (CodeLens lens : lenses)
            lens.setData(data);
        return lenses;
    }

    /**
     * Resolves a code lens
     *
     * @param lens The code lens to resolve
     * @return The resolved code lens
     */
    public CodeLens resolveCodeLens(CodeLens lens) {
        if (lens.getData() == null)
            return lens;
        Document document = documents.get(lens.getData());
        if (document == null)
            return lens;
        DocumentLensProvider service = getServiceLensProvider(document);
        if (service == null)
            return lens;
        return service.resolve(lens);
    }

    /**
     * Gets the document links for the specified parameters
     *
     * @param parameters The parameters for this request
     * @return The document links
     */
    public DocumentLink[] getDocumentLinks(DocumentLinkParams parameters) {
        Document document = documents.get(parameters.getTextDocument().getUri());
        if (document == null)
            return new DocumentLink[0];
        DocumentAnalysis analysis = document.getLastAnalysis();
        if (analysis == null)
            return new DocumentLink[0];
        return analysis.getLinks().toArray(new DocumentLink[0]);
    }

    /**
     * Resolves a document link
     *
     * @param link The document link to resolve
     * @return The resolved document link
     */
    public DocumentLink resolveDocumentLink(DocumentLink link) {
        return link;
    }

    /**
     * Renames all occurrences of a symbol within this workspace
     *
     * @param parameters The parameters for the operation
     * @return The operation's result as a list of edits to be applied to documents in this workspace
     */
    public WorkspaceEdit renameSymbol(RenameParams parameters) {
        Document document = documents.get(parameters.getTextDocument().getUri());
        if (document == null)
            return null;
        Symbol symbol = symbolRegistry.getSymbolAt(parameters.getTextDocument().getUri(), parameters.getPosition());
        if (symbol == null)
            return null;
        DocumentSymbolHandler service = getServiceSymbolHandler(document);
        if (service == null)
            return null;
        if (!service.isLegalName(document, symbolRegistry, symbol, parameters.getNewName()))
            return null;
        Map<Document, TextEdit[]> edits = new HashMap<>();
        for (String uri : symbol.getDefiningDocuments()) {
            Document doc = documents.get(uri);
            if (doc == null)
                return null;
            service = getServiceSymbolHandler(doc);
            if (service == null)
                return null;
            TextEdit[] changes = service.rename(doc, symbol, parameters.getNewName());
            if (changes == null)
                return null;
            if (changes.length > 0)
                edits.put(doc, changes);
        }
        for (String uri : symbol.getReferencingDocuments()) {
            if (symbol.getDefiningDocuments().contains(uri))
                continue;
            Document doc = documents.get(uri);
            if (doc == null)
                return null;
            service = getServiceSymbolHandler(doc);
            if (service == null)
                return null;
            TextEdit[] changes = service.rename(doc, symbol, parameters.getNewName());
            if (changes == null)
                return null;
            if (changes.length > 0)
                edits.put(doc, changes);
        }

        WorkspaceEdit result = new WorkspaceEdit();
        if (local != null && local instanceof LspServer && ((LspServer) local).getClientCapabiltiies().supports("workspace.workspaceEdit.documentChanges")) {
            for (Map.Entry<Document, TextEdit[]> entry : edits.entrySet()) {
                result.addChanges(entry.getKey().getUri(), entry.getKey().getCurrentVersion().getNumber(), entry.getValue());
            }
        } else {
            for (Map.Entry<Document, TextEdit[]> entry : edits.entrySet()) {
                result.addChanges(entry.getKey().getUri(), entry.getValue());
            }
        }
        return result;
    }

    /**
     * Lists the server capabilities that are supported by services provided by this entity
     *
     * @param capabilities The server capabilities structure to fill
     */
    protected void listServerCapabilities(ServerCapabilities capabilities) {
        // do nothing
    }

    /**
     * Lists the client capabilities that are supported by services provided by this entity
     *
     * @param capabilities The client capabilities structure to fill
     */
    protected void listClientCapabilities(ClientCapabilities capabilities) {
        // do nothing
    }

    /**
     * Gets the document analyzer service for the specified document
     *
     * @param document A document
     * @return The corresponding document analyzer
     */
    protected DocumentAnalyzer getServiceAnalyzer(Document document) {
        return null;
    }

    /**
     * Gets the document completer service for the specified document
     *
     * @param document A document
     * @return The corresponding document completer
     */
    protected DocumentCompleter getServiceCompleter(Document document) {
        return null;
    }

    /**
     * Gets the document hover provider service for the specified document
     *
     * @param document A document
     * @return The corresponding document hover provider
     */
    protected DocumentHoverProvider getServiceHoverProvider(Document document) {
        return null;
    }

    /**
     * Gets the document signature helper for the specified document
     *
     * @param document A document
     * @return The corresponding document signature helper
     */
    protected DocumentSignatureHelper getServiceSignatureHelp(Document document) {
        return null;
    }

    /**
     * Gets the document formatter for the specified document
     *
     * @param document A document
     * @return The corresponding document formatter
     */
    protected DocumentFormatter getServiceFormatter(Document document) {
        return null;
    }

    /**
     * Gets the document action provider for the specified document
     *
     * @param document A document
     * @return The corresponding document action provider
     */
    protected DocumentActionProvider getServiceActionProvider(Document document) {
        return null;
    }

    /**
     * Gets the document lens provider for the specified document
     *
     * @param document A document
     * @return The corresponding document lens provider
     */
    protected DocumentLensProvider getServiceLensProvider(Document document) {
        return null;
    }

    /**
     * Gets the document symbol handler for the specified document
     *
     * @param document A document
     * @return The corresponding document symbol handler
     */
    protected DocumentSymbolHandler getServiceSymbolHandler(Document document) {
        return null;
    }
}
