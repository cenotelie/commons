/*******************************************************************************
 * Copyright (c) 2016 Association Cénotélie (cenotelie.fr)
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

package fr.cenotelie.commons.utils.product;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Utility API for reading manifests
 *
 * @author Laurent Wouters
 */
public class ManifestUtils {
    /**
     * The symbolic name of the bundle
     */
    public static final String BUNDLE_SYMBOLIC_NAME = "Bundle-SymbolicName";
    /**
     * The human-readable name of the bundle
     */
    public static final String BUNDLE_NAME = "Bundle-Name";
    /**
     * The description of the bundle
     */
    public static final String BUNDLE_DESCRIPTION = "Bundle-Description";
    /**
     * The version of the bundle
     */
    public static final String BUNDLE_VERSION = "Bundle-Version";
    /**
     * The bundle's vendor
     */
    public static final String BUNDLE_VENDOR = "Bundle-Vendor";
    /**
     * The URL to the bundle's documentation
     */
    public static final String BUNDLE_DOCURL = "Bundle-DocURL";
    /**
     * The list of embedded dependencies in this bundle
     */
    public static final String BUNDLE_DEPENDENCIES = "Bundle-Dependencies";
    /**
     * The URL to the product description for this bundle
     */
    public static final String BUNDLE_PRODUCT_LINK = "Bundle-Product-Link";
    /**
     * The name if the license for this bundle
     */
    public static final String BUNDLE_LICENSE_NAME = "Bundle-License-Name";
    /**
     * The resource that contains the license for this bundle
     */
    public static final String BUNDLE_LICENSE_RESOURCE = "Bundle-License-Resource";
    /**
     * The hash of the commit in the SCM that was used to build this bundle
     */
    public static final String BUNDLE_SCM_TAG = "Bundle-SCM-Tag";
    /**
     * The tag of the build that produced this bundle
     */
    public static final String BUNDLE_BUILD_TAG = "Bundle-Build-Tag";
    /**
     * The timestamp when this bundle was built
     */
    public static final String BUNDLE_BUILD_TIMESTAMP = "Bundle-Build-Timestamp";
    /**
     * The user that built this bundle
     */
    public static final String BUILTBY = "Built-By";

    /**
     * Gets the manifest for the jar of the specified type
     *
     * @param type A type
     * @return The manifest for the jar that contains the type
     * @throws IOException When a resource cannot be read
     */
    public static Manifest getManifest(Class<?> type) throws IOException {
        String target = type.getResource(type.getSimpleName() + ".class").toString();
        target = target.substring(0, target.length() - type.getSimpleName().length() - ".class".length() - 1 - type.getPackage().getName().length());
        target = target + JarFile.MANIFEST_NAME;
        URL url = new URL(target);

        try (InputStream stream = url.openStream()) {
            return new Manifest(stream);
        }
    }
}
