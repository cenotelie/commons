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

/**
 * Represents a service for a document
 *
 * @author Laurent Wouters
 */
public interface DocumentService {
    /**
     * Priority when the service is not applicable
     */
    int PRIORITY_NONE = -1;
    /**
     * The minimal priority for an appropriate service
     */
    int PRIORITY_MINIMAL = 0;
    /**
     * An average priority for an appropriate service
     */
    int PRIORITY_AVERAGE = 50;
    /**
     * A high priority for an appropriate service
     */
    int PRIORITY_HIGH = 100;

    /**
     * Gets the priority of this service for the specified document
     * A greater number indicates a greater priority.
     * A negative number indicates that the service is not appropriate for the document
     *
     * @param document The document
     * @return The priority
     */
    int getPriorityFor(Document document);
}
