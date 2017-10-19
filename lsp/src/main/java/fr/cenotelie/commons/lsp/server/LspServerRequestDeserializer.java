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

import fr.cenotelie.commons.lsp.structures.*;
import fr.cenotelie.commons.utils.json.JsonDeserializer;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * A de-serializer for the request objects received by a LSP server
 *
 * @author Laurent Wouters
 */
public class LspServerRequestDeserializer extends JsonDeserializer {
    @Override
    public Object deserializeObject(ASTNode definition, Object context) {
        if (context != null && (context instanceof String))
            return deserializeObject(definition, (String) context);
        return super.deserializeObject(definition, context);
    }

    /**
     * De-serializes an object related to a request
     *
     * @param definition The serialized parameters
     * @param method     The current LSP method
     * @return The de-serialized object
     */
    public Object deserializeObject(ASTNode definition, String method) {
        switch (method) {
            case "initialize":
                return new InitializeParams(definition, this);
            case "$/cancelRequest":
                return new CancelParams(definition);
            case "workspace/didChangeConfiguration":
                return new DidChangeConfigurationParams(definition, this);
            case "workspace/didChangeWatchedFiles":
                return new DidChangeWatchedFilesParams(definition);
            case "workspace/symbol":
                return new WorkspaceSymbolParams(definition);
            case "workspace/executeCommand":
                return new ExecuteCommandParams(definition, this);
            case "textDocument/didOpen":
                return new DidOpenTextDocumentParams(definition);
            case "textDocument/didChange":
                return new DidChangeTextDocumentParams(definition);
            case "textDocument/willSave":
                return new WillSaveTextDocumentParams(definition);
            case "textDocument/willSaveWaitUntil":
                return new WillSaveTextDocumentParams(definition);
            case "textDocument/didSave":
                return new DidSaveTextDocumentParams(definition);
            case "textDocument/didClose":
                return new DidCloseTextDocumentParams(definition);
            case "textDocument/completion":
                return new TextDocumentPositionParams(definition);
            case "completionItem/resolve":
                return new CompletionItem(definition, this);
            case "textDocument/hover":
                return new TextDocumentPositionParams(definition);
            case "textDocument/signatureHelp":
                return new TextDocumentPositionParams(definition);
            case "textDocument/references":
                return new ReferenceParams(definition);
            case "textDocument/documentHighlight":
                return new TextDocumentPositionParams(definition);
            case "textDocument/documentSymbol":
                return new DocumentSymbolParams(definition);
            case "textDocument/formatting":
                return new DocumentFormattingParams(definition, this);
            case "textDocument/rangeFormatting":
                return new DocumentRangeFormattingParams(definition, this);
            case "textDocument/onTypeFormatting":
                return new DocumentOnTypeFormattingParams(definition, this);
            case "textDocument/definition":
                return new TextDocumentPositionParams(definition);
            case "textDocument/codeAction":
                return new CodeActionParams(definition);
            case "textDocument/codeLens":
                return new CodeLensParams(definition);
            case "codeLens/resolve":
                return new CodeLens(definition, this);
            case "textDocument/documentLink":
                return new DocumentLinkParams(definition);
            case "documentLink/resolve":
                return new DocumentLink(definition);
            case "textDocument/rename":
                return new RenameParams(definition);
        }
        return super.deserializeObject(definition, method);
    }
}
