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

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * The provider of document content implementations
 *
 * @author Laurent Wouters
 */
class DocumentContentProvider implements DocumentContentFactory {
    /**
     * The singleton instance
     */
    private static final DocumentContentProvider INSTANCE = new DocumentContentProvider();

    /**
     * The Java service loader for the factories
     */
    private final ServiceLoader<DocumentContentFactory> javaProvider;

    /**
     * Initialize this provider
     */
    private DocumentContentProvider() {
        this.javaProvider = ServiceLoader.load(DocumentContentFactory.class);
    }

    /**
     * Creates a new document content object
     *
     * @param text The initial text for the document content
     * @return The document content object
     */
    public static DocumentContent getContent(String text) {
        return INSTANCE.newContent(text);
    }

    @Override
    public DocumentContent newContent(String text) {
        Iterator<DocumentContentFactory> services = javaProvider.iterator();
        if (services.hasNext())
            return services.next().newContent(text);

        Bundle bundle = FrameworkUtil.getBundle(DocumentContentProvider.class);
        if (bundle == null)
            return new DocumentContentString(text);
        BundleContext context = FrameworkUtil.getBundle(DocumentContentProvider.class).getBundleContext();
        if (context == null)
            return new DocumentContentString(text);
        ServiceReference reference = context.getServiceReference(DocumentContentFactory.class);
        if (reference == null)
            return new DocumentContentString(text);
        DocumentContentFactory result = (DocumentContentFactory) context.getService(reference);
        context.ungetService(reference);
        return result.newContent(text);
    }
}
