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

package fr.cenotelie.commons.lsp.structures;

import fr.cenotelie.commons.utils.Serializable;
import fr.cenotelie.commons.utils.json.Json;
import fr.cenotelie.hime.redist.ASTNode;

/**
 * The response to a 'will save wait until' request
 *
 * @author Laurent Wouters
 */
public class WillSaveWaitUntilTextDocumentResponse implements Serializable {
    /**
     * The edits to be applied by the client
     */
    private final TextEdit[] edits;

    /**
     * Gets the edits to be applied by the client
     *
     * @return The edits to be applied by the client
     */
    public TextEdit[] getEdits() {
        return edits;
    }

    /**
     * Gets the number of edits
     *
     * @return the number of edits
     */
    public int size() {
        return edits.length;
    }

    /**
     * Gets the i-th item
     *
     * @param index The index
     * @return The i-th item
     */
    public TextEdit get(int index) {
        return edits[index];
    }

    /**
     * Initializes this structure
     *
     * @param edits The edits to be applied by the client
     */
    public WillSaveWaitUntilTextDocumentResponse(TextEdit[] edits) {
        this.edits = edits;
    }

    /**
     * Initializes this structure
     *
     * @param definition The serialized definition
     */
    public WillSaveWaitUntilTextDocumentResponse(ASTNode definition) {
        this.edits = new TextEdit[definition.getChildren().size()];
        int index = 0;
        for (ASTNode child : definition.getChildren())
            edits[index++] = new TextEdit(child);
    }

    @Override
    public String serializedString() {
        return serializedJSON();
    }

    @Override
    public String serializedJSON() {
        return Json.serialize(edits);
    }
}
