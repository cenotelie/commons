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

package fr.cenotelie.commons.p2uploader;

import fr.cenotelie.commons.utils.IOUtils;
import fr.cenotelie.commons.utils.http.HttpConnection;
import fr.cenotelie.commons.utils.http.HttpConstants;
import fr.cenotelie.commons.utils.http.HttpResponse;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.apache.maven.settings.Server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A mojo to upload an Eclipse Update Site to a Nexus raw repository
 *
 * @author Laurent Wouters
 */
@Mojo(name = "upload-p2", defaultPhase = LifecyclePhase.DEPLOY)
public class UploadP2Mojo extends AbstractMojo {
    /**
     * The current Maven session
     */
    @Parameter(property = "session", readonly = true)
    private MavenSession session;
    /**
     * The current Maven project
     */
    @Parameter(readonly = true, defaultValue = "${project}", required = true)
    protected MavenProject project;
    /**
     * The name of the repository folder in the project's build directory
     */
    @Parameter(defaultValue = "repository", required = true)
    protected String repositoryFolder;
    /**
     * The identifier of the server configuration to use
     */
    @Parameter(required = true)
    protected String serverId;
    /**
     * The target URL to upload the update
     */
    @Parameter(required = true)
    protected String targetUrl;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        File repository = new File(project.getModel().getBuild().getDirectory(), repositoryFolder);
        if (!repository.exists())
            throw new MojoFailureException("The repository folder does not exist (" + repository.toString() + ")");
        Server server = session.getSettings().getServer(serverId);
        if (server == null)
            throw new MojoFailureException("Undefined server: " + serverId);
        HttpConnection connection = new HttpConnection(
                targetUrl.endsWith("/") ? targetUrl : targetUrl + "/",
                server.getUsername(),
                server.getPassword());
        upload(repository, connection, "");
    }

    /**
     * Uploads a file or a directory
     *
     * @param file       The file or directory to upload
     * @param connection The connection to use
     * @param prefix     The current prefix for this file or directory
     * @throws MojoFailureException When a file failed to be uploaded
     */
    private void upload(File file, HttpConnection connection, String prefix) throws MojoFailureException {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    upload(child, connection, file.getName() + "/");
                }
            }
        } else {
            String fileName = file.getAbsolutePath();
            getLog().info("Uploading " + fileName);
            try (InputStream stream = new FileInputStream(file)) {
                byte[] content = IOUtils.load(stream);
                HttpResponse response = connection.request(
                        prefix + file.getName(),
                        HttpConstants.METHOD_PUT,
                        content,
                        HttpConstants.MIME_OCTET_STREAM,
                        false,
                        HttpConstants.MIME_TEXT_PLAIN);
                if (response.getCode() >= 300 || response.getCode() < 200)
                    throw new MojoFailureException("Failed to upload " + fileName);
            } catch (IOException exception) {
                throw new MojoFailureException("Failed to upload " + fileName, exception);
            }
        }
    }
}
