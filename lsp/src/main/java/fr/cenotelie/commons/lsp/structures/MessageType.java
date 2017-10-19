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
 * The types of messages
 *
 * @author Laurent Wouters
 */
public interface MessageType {
    /**
     * An error message.
     */
    int TYPE_ERROR = 1;
    /**
     * A warning message.
     */
    int TYPE_WARNING = 2;
    /**
     * An information message.
     */
    int TYPE_INFO = 3;
    /**
     * A log message.
     */
    int TYPE_LOG = 4;
}
