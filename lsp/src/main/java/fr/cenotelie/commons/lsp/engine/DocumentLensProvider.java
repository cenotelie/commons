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

import fr.cenotelie.commons.lsp.structures.CodeLens;

/**
 * A document service that provide code lens
 *
 * @author Laurent Wouters
 */
public interface DocumentLensProvider extends DocumentService {
    /**
     * Gets code lens for the specified document
     *
     * @param document The document
     * @return The code lens
     */
    CodeLens[] getLens(Document document);

    /**
     * Resolves a code lens
     *
     * @param codeLens A code lens
     * @return The resolved code lens
     */
    CodeLens resolve(CodeLens codeLens);
}
