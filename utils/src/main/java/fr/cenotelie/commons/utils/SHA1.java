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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility APIs for SHA1 hashing
 *
 * @author Laurent Wouters
 */
public class SHA1 {
    /**
     * Hexadecimal characters
     */
    private static final char[] HEXA_CHARS = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    /**
     * Computes the SHA1 hash of a string and serializes this hash as a string
     *
     * @param input The string to hash
     * @return The SHA1 hash as a string
     */
    public static String hashSHA1(String input) {
        return hashSHA1(input.getBytes(IOUtils.CHARSET));
    }

    /**
     * Computes the SHA1 hash of bytes and serializes this hash as a string
     *
     * @param input The bytes to hash
     * @return The SHA1 hash as a string
     */
    public static String hashSHA1(byte[] input) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
            byte[] bytes = md.digest(input);
            char[] chars = new char[bytes.length * 2];
            int j = 0;
            for (int i = 0; i != bytes.length; i++) {
                chars[j++] = HEXA_CHARS[(bytes[i] & 0xF0) >>> 4];
                chars[j++] = HEXA_CHARS[bytes[i] & 0x0F];
            }
            return new String(chars);
        } catch (NoSuchAlgorithmException exception) {
            Logging.get().error(exception);
            return "";
        }
    }
}
