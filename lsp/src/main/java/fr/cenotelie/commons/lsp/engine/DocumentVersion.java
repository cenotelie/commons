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

import fr.cenotelie.commons.lsp.structures.TextDocumentContentChangeEvent;
import fr.cenotelie.commons.lsp.structures.TextEdit;

/**
 * Represents a specific document in a specific version
 *
 * @author Laurent Wouters
 */
public class DocumentVersion {
    /**
     * The version number for this version
     */
    private final int number;
    /**
     * The content of the document in this version
     */
    private final DocumentContent content;

    /**
     * Initializes this version
     *
     * @param number  The version number for this version
     * @param content The content of the document in this version
     */
    public DocumentVersion(int number, DocumentContent content) {
        this.number = number;
        this.content = content;
    }

    /**
     * Gets the version number for this document version
     *
     * @return The version number
     */
    public int getNumber() {
        return number;
    }

    /**
     * Gets the content of the document in this version
     *
     * @return The content of the document in this version
     */
    public DocumentContent getContent() {
        return content;
    }

    /**
     * From this version, applies the specified edits to mutate into a new version
     *
     * @param nextNumber The new version number
     * @param edits      The edits to be applied
     * @return The new version
     */
    public DocumentVersion mutateTo(int nextNumber, TextEdit[] edits) {
        return new DocumentVersion(nextNumber, content.cloneWith(edits));
    }

    /**
     * From this version, applies the specified edits to mutate into a new version
     *
     * @param nextNumber The new version number
     * @param events     The events to be applied
     * @return The new version
     */
    public DocumentVersion mutateTo(int nextNumber, TextDocumentContentChangeEvent[] events) {
        if (events == null || events.length == 0)
            return new DocumentVersion(nextNumber, content);
        for (int i = 0; i != events.length; i++) {
            if (events[i].isFullReplace()) {
                if (events.length != 1) {
                    // invalid
                    return null;
                } else {
                    return new DocumentVersion(nextNumber, DocumentContentProvider.getContent(events[1].getText()));
                }
            }
        }
        TextEdit[] edits = new TextEdit[events.length];
        for (int i = 0; i != events.length; i++)
            edits[i] = events[i].toEdit();
        return mutateTo(nextNumber, edits);
    }
}
