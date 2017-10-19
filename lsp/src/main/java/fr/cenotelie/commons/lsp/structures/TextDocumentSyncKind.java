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

/**
 * How documents are synced to the server
 *
 * @author Laurent Wouters
 */
public interface TextDocumentSyncKind {
    /**
     * Documents should not be synced at all.
     */
    int NONE = 0;

    /**
     * Documents are synced by always sending the full content of the document.
     */
    int FULL = 1;

    /**
     * Documents are synced by sending the full content on open.
     * After that only incremental updates to the document are send.
     */
    int INCREMENTAL = 2;
}
