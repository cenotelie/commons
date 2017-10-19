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

import org.osgi.framework.*;

import java.util.Collection;
import java.util.WeakHashMap;

/**
 * A provider of document services that uses the OSGi service resolver to find the services
 *
 * @author Laurent Wouters
 */
public class DocumentServiceProviderOSGi<T extends DocumentService> implements DocumentServiceProvider<T> {
    /**
     * The map of the resolved services
     */
    private final WeakHashMap<Document, T> resolvedAnalyzers;
    /**
     * The type of the resolve services
     */
    private final Class<T> serviceType;

    /**
     * Initializes this provider
     *
     * @param serviceType The type of the resolve services
     */
    public DocumentServiceProviderOSGi(Class<T> serviceType) {
        this.resolvedAnalyzers = new WeakHashMap<>();
        this.serviceType = serviceType;
    }

    @Override
    public T getService(Document document) {
        T best = resolvedAnalyzers.get(document);
        if (best != null)
            return best;

        int bestPriority = -1;

        Bundle bundle = FrameworkUtil.getBundle(DocumentContentProvider.class);
        if (bundle != null) {
            BundleContext context = FrameworkUtil.getBundle(DocumentContentProvider.class).getBundleContext();
            if (context != null) {
                try {
                    Collection<ServiceReference<T>> references = context.getServiceReferences(serviceType, null);
                    for (ServiceReference<T> reference : references) {
                        T analyzer = context.getService(reference);
                        context.ungetService(reference);
                        int priority = analyzer.getPriorityFor(document);
                        if (priority > bestPriority) {
                            best = analyzer;
                            bestPriority = priority;
                        }
                    }
                } catch (InvalidSyntaxException exception) {
                    // cannot happen
                }
            }
        }
        if (best != null)
            resolvedAnalyzers.put(document, best);
        return best;
    }
}
