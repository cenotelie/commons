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

import fr.cenotelie.commons.utils.IOUtils;
import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.commons.utils.logging.Logging;

import java.io.IOException;
import java.io.InputStream;

/**
 * Implements a license that is embedded as a resource in a jar file
 *
 * @author Laurent Wouters
 */
public class LicenseEmbedded implements License {
    /**
     * A class in the same jar as the resource
     */
    private final Class<?> type;
    /**
     * The resource that contains the license's full text
     */
    private final String resource;
    /**
     * The license's name
     */
    private final String name;
    /**
     * The license's full text
     */
    private String fullText;

    /**
     * Initialize this structure
     *
     * @param name     The license's full text
     * @param type     A class in the same jar as the resource
     * @param resource The resource that contains the license's full text
     */
    public LicenseEmbedded(String name, Class<?> type, String resource) {
        this.name = name;
        this.type = type;
        this.resource = resource;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public synchronized String getFullText() {
        if (fullText == null) {
            try (InputStream stream = type.getResourceAsStream(resource)) {
                fullText = IOUtils.read(stream, IOUtils.CHARSET);
            } catch (IOException exception) {
                Logging.get().error(exception);
            }
        }
        return fullText;
    }

    @Override
    public String serializedString() {
        return name;
    }

    @Override
    public String serializedJSON() {
        return "{\"type\": \"" +
                TextUtils.escapeStringJSON(License.class.getCanonicalName()) +
                "\", \"name\": \"" +
                TextUtils.escapeStringJSON(name) +
                "\", \"fullText\": \"" +
                TextUtils.escapeStringJSON(getFullText()) +
                "\"}";
    }
}
