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

import fr.cenotelie.commons.lsp.structures.Hover;
import fr.cenotelie.commons.lsp.structures.Position;

/**
 * Represents an entity that can provide hover information for a document
 *
 * @author Laurent Wouters
 */
public interface DocumentHoverProvider extends DocumentService {
    /**
     * Gets the hover data for a document at a position
     *
     * @param document The document
     * @param position The position within the document
     * @return The hover data
     */
    Hover getHoverData(Document document, Position position);
}
