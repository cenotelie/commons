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
import fr.cenotelie.commons.utils.Identifiable;
import fr.cenotelie.commons.utils.Serializable;
import fr.cenotelie.commons.utils.TextUtils;
import fr.cenotelie.commons.utils.config.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.jar.Manifest;

/**
 * Represents an embedded dependency for the current product
 *
 * @author Laurent Wouters
 */
public class EmbeddedDependency implements Identifiable, Serializable {
    /**
     * The dependency's identifier
     */
    private final String identifier;
    /**
     * The type of embedded content
     */
    private final String contentType;
    /**
     * The dependency's name
     */
    private final String name;
    /**
     * The dependency's version
     */
    private final String version;
    /**
     * The dependency's copyright
     */
    private final String copyright;
    /**
     * A link to the dependency's web site
     */
    private final String link;
    /**
     * The license for the dependency
     */
    private final License license;

    /**
     * Initializes this structure
     *
     * @param type       A class in the same jar as the resource
     * @param identifier The dependency's identifier
     * @throws IOException When the resource cannot be read
     */
    public EmbeddedDependency(Class<?> type, String identifier) throws IOException {
        try (InputStream stream = type.getResourceAsStream("/META-INF/dependencies/" + identifier)) {
            Configuration configuration = new Configuration();
            configuration.load(stream, IOUtils.CHARSET);
            this.identifier = identifier;
            this.contentType = configuration.get("type");
            this.name = configuration.get("name");
            this.version = configuration.get("version");
            this.copyright = configuration.get("copyright");
            this.link = configuration.get("link");
            this.license = new LicenseEmbedded(
                    configuration.get("licenseName"),
                    type,
                    configuration.get("licenseText"));
        }
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Gets the type of embedded content
     *
     * @return The type of embedded content
     */
    public String getContentType() {
        return contentType;
    }

    /**
     * Gets the dependency's version
     *
     * @return The dependency's version
     */
    public String getVersion() {
        return version;
    }

    /**
     * Gets the dependency's copyright
     *
     * @return The dependency's copyright
     */
    public String getCopyright() {
        return copyright;
    }

    /**
     * Gets a link to the dependency's web site
     *
     * @return A link to the dependency's web site
     */
    public String getLink() {
        return link;
    }

    /**
     * Gets the license for the dependency
     *
     * @return The license for the dependency
     */
    public License getLicense() {
        return license;
    }

    @Override
    public String serializedString() {
        return identifier;
    }

    @Override
    public String serializedJSON() {
        return "{\"type\": \"" +
                TextUtils.escapeStringJSON(EmbeddedDependency.class.getCanonicalName()) +
                "\", \"identifier\": \"" +
                TextUtils.escapeStringJSON(identifier) +
                "\", \"name\": \"" +
                TextUtils.escapeStringJSON(name) +
                "\", \"contentType\": \"" +
                TextUtils.escapeStringJSON(contentType) +
                "\", \"version\": \"" +
                TextUtils.escapeStringJSON(version) +
                "\", \"copyright\": \"" +
                TextUtils.escapeStringJSON(copyright) +
                "\", \"link\": \"" +
                TextUtils.escapeStringJSON(link) +
                "\", \"license\": " +
                license.serializedJSON() +
                "}";
    }

    /**
     * Gets the dependencies embedded in the same jar as the specified type
     *
     * @param type A type
     * @return The embedded dependencies
     * @throws IOException When the resource cannot be read
     */
    public static Collection<EmbeddedDependency> getDependenciesFor(Class<?> type) throws IOException {
        Manifest manifest = ManifestUtils.getManifest(type);
        String value = manifest.getMainAttributes().getValue("XOWL-Dependencies");
        Collection<EmbeddedDependency> result = new ArrayList<>();
        if (value != null) {
            String[] identifiers = value.split(";");
            for (String identifier : identifiers) {
                result.add(new EmbeddedDependency(type, identifier.trim()));
            }
        }
        return result;
    }
}
