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
