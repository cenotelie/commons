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

package fr.cenotelie.commons.utils;

import fr.cenotelie.commons.utils.logging.Logging;

import java.io.*;
import java.security.SecureRandom;
import java.util.Scanner;

/**
 * Generator of SSL key stores
 *
 * @author Laurent Wouters
 */
public class SSLGenerator {
    /**
     * Generates a key store with a new self-signed certificate
     *
     * @param target The target file for the key store
     * @param alias  Common name for the certificate
     * @return The password to the key store
     */
    public static String generateKeyStore(File target, String alias) {
        SecureRandom random = new SecureRandom();
        byte[] buffer = new byte[20];
        random.nextBytes(buffer);
        String password = SHA1.hashSHA1(buffer);

        try {
            if (target.exists() && !target.delete()) {
                // failed to delete
                return null;
            }
            String[] command = new String[]{"keytool", "-genkeypair",
                    "-alias", alias,
                    "-keyalg", "RSA", "-keysize", "2048",
                    "-dname", "CN=" + alias + ", O=" + alias,
                    "-validity", "3650", "-storetype", "JKS",
                    "-keystore", target.getAbsolutePath()};
            final Process process = Runtime.getRuntime().exec(command);
            new Thread(new Runnable() {
                public void run() {
                    try (InputStream inStream = process.getInputStream()) {
                        InputStreamReader reader = new InputStreamReader(inStream);
                        Scanner scan = new Scanner(reader);
                        while (scan.hasNextLine()) {
                            System.out.println(scan.nextLine());
                        }
                    } catch (IOException exception) {
                        Logging.get().error(exception);
                    }
                }
            }).start();
            try (OutputStream outStream = process.getOutputStream()) {
                PrintWriter writer = new PrintWriter(outStream);
                writer.println(password);
                writer.flush();
                writer.println(password);
                writer.flush();
                writer.println();
                writer.flush();
                writer.close();
            }
            process.waitFor();
            return password;
        } catch (IOException | InterruptedException exception) {
            Logging.get().error(exception);
            return null;
        }
    }
}
