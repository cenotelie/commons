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

package fr.cenotelie.commons.utils.api;

import fr.cenotelie.commons.utils.Serializable;

/**
 * Represents a reply to a request in an API
 *
 * @author Laurent Wouters
 */
public interface Reply extends Serializable {
    /**
     * Gets whether the reply is a success
     *
     * @return Whether the reply is a success
     */
    boolean isSuccess();

    /**
     * Gets the message, if any
     *
     * @return The message, if any
     */
    String getMessage();
}
