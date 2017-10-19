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

import fr.cenotelie.commons.lsp.structures.TextEdit;

import java.io.Reader;

/**
 * Represents the content of a document
 *
 * @author Laurent Wouters
 */
public interface DocumentContent {
    /**
     * Gets a reader for this content
     *
     * @return The reader for this content
     */
    Reader getReader();

    /**
     * Applies the specified edits in place
     *
     * @param edits The edits to be applied
     */
    void applyEdits(TextEdit[] edits);

    /**
     * Clones this content with the specified edits applied
     *
     * @param edits The edits to be applied in the clone
     * @return The clone
     */
    DocumentContent cloneWith(TextEdit[] edits);
}
