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
 * A static provider of services
 *
 * @author Laurent Wouters
 */
public class DocumentServiceProviderStatic<T extends DocumentService> implements DocumentServiceProvider<T> {
    /**
     * The provided services
     */
    private final T[] services;

    /**
     * Initializes this provider
     *
     * @param services The provided services
     */
    @SafeVarargs
    public DocumentServiceProviderStatic(T... services) {
        this.services = services;
    }

    @Override
    public T getService(Document document) {
        T best = null;
        int bestPriority = -1;
        for (int i = 0; i != services.length; i++) {
            int priority = services[i].getPriorityFor(document);
            if (priority > bestPriority) {
                best = services[i];
                bestPriority = priority;
            }
        }
        return best;
    }
}
