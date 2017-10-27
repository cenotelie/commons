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

import fr.cenotelie.commons.utils.Serializable;
import fr.cenotelie.commons.utils.TextUtils;

import java.util.jar.Manifest;

/**
 * Represents the information about the version of a product or a component
 *
 * @author Laurent Wouters
 */
public class VersionInfo implements Serializable {
    /**
     * The version number
     */
    private final String number;
    /**
     * The SCM tag indicating the source version
     */
    private final String scmTag;
    /**
     * The name of the user that performed the build for this version
     */
    private final String buildUser;
    /**
     * The tag for the build that produced this version
     */
    private final String buildTag;
    /**
     * The timestamp for the build that produced this version
     */
    private final String buildTimestamp;

    /**
     * Initializes this version info
     *
     * @param manifest The original manifest
     */
    public VersionInfo(Manifest manifest) {
        this.number = manifest.getMainAttributes().getValue(ManifestUtils.BUNDLE_VERSION);
        this.scmTag = manifest.getMainAttributes().getValue(ManifestUtils.X_SCM_TAG);
        this.buildUser = manifest.getMainAttributes().getValue(ManifestUtils.BUILTBY);
        this.buildTag = manifest.getMainAttributes().getValue(ManifestUtils.X_BUILD_TAG);
        this.buildTimestamp = manifest.getMainAttributes().getValue(ManifestUtils.X_BUILD_TIMESTAMP);
    }

    /**
     * Initializes this version info
     *
     * @param number         The version number
     * @param scmTag         The SCM tag indicating the source version
     * @param buildUser      The name of the user that performed the build for this version
     * @param buildTag       The tag for the build that produced this version
     * @param buildTimestamp The timestamp for the build that produced this version
     */
    public VersionInfo(String number, String scmTag, String buildUser, String buildTag, String buildTimestamp) {
        this.number = number;
        this.scmTag = scmTag;
        this.buildUser = buildUser;
        this.buildTag = buildTag;
        this.buildTimestamp = buildTimestamp;
    }

    /**
     * Gets the version number
     *
     * @return The version number
     */
    public String getNumber() {
        return number;
    }

    /**
     * Gets the SCM tag indicating the source version
     *
     * @return The SCM tag indicating the source version
     */
    public String getScmTag() {
        return scmTag;
    }

    /**
     * Gets the name of the user that performed the build for this version
     *
     * @return The name of the user that performed the build for this version
     */
    public String getBuildUser() {
        return buildUser;
    }

    /**
     * Gets the tag for the build that produced this version
     *
     * @return The tag for the build that produced this version
     */
    public String getBuildTag() {
        return buildTag;
    }

    /**
     * Gets the timestamp for the build that produced this version
     *
     * @return The timestamp for the build that produced this version
     */
    public String getBuildTimestamp() {
        return buildTimestamp;
    }

    @Override
    public String serializedString() {
        return number;
    }

    @Override
    public String serializedJSON() {
        return "{\"number\": \"" +
                TextUtils.escapeStringJSON(number) +
                "\", \"scmTag\": \"" +
                (scmTag != null ? TextUtils.escapeStringJSON(scmTag) : "") +
                "\", \"buildUser\": \"" +
                (buildUser != null ? TextUtils.escapeStringJSON(buildUser) : "") +
                "\", \"buildTag\": \"" +
                (buildTag != null ? TextUtils.escapeStringJSON(buildTag) : "") +
                "\", \"buildTimestamp\": \"" +
                (buildTimestamp != null ? TextUtils.escapeStringJSON(buildTimestamp) : "") +
                "\"}";
    }
}
