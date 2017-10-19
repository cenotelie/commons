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
 * Represents reasons why a text document is saved
 *
 * @author Laurent Wouters
 */
public interface TextDocumentSaveReason {
    /**
     * Manually triggered, e.g. by the user pressing save, by starting debugging, or by an API call.
     */
    int MANUAL = 1;

    /**
     * Automatic after a delay.
     */
    int AFTER_DELAY = 2;

    /**
     * When the editor lost focus.
     */
    int FOCUS_OUT = 3;
}
