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

import fr.cenotelie.commons.lsp.structures.*;

/**
 * The default hover data provider that is based on the found symbols
 *
 * @author Laurent Wouters
 */
public class DocumentHoverProviderDefault implements DocumentHoverProvider {
    /**
     * The parent workspace
     */
    private final Workspace workspace;

    /**
     * Initializes this service
     *
     * @param workspace The parent workspace
     */
    public DocumentHoverProviderDefault(Workspace workspace) {
        this.workspace = workspace;
    }

    @Override
    public int getPriorityFor(Document document) {
        return PRIORITY_MINIMAL;
    }

    @Override
    public Hover getHoverData(Document document, Position position) {
        Symbol symbol = workspace.getSymbols().getSymbolAt(document.getUri(), position);
        MarkupContent documentation = symbol.getDocumentation();
        Range range = symbol.getRangeAt(document.getUri(), position);
        return new Hover(
                documentation == null ? new MarkupContent(MarkupKind.PLAIN_TEXT, "") : documentation,
                range
        );
    }
}
