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

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility API for IO management
 *
 * @author Laurent Wouters
 */
public class IOUtils {
    /**
     * The line separator for all IO operations
     */
    public static final String LINE_SEPARATOR = "\n";
    /**
     * The UTF-8 charset
     */
    public static final Charset UTF8 = Charset.forName("UTF-8");
    /**
     * The default charset
     */
    public static final Charset CHARSET = UTF8;
    /**
     * The size of buffers for loading content
     */
    private static final int BUFFER_SIZE = 1024;

    /**
     * Gets a writer for the specified file
     *
     * @param file A file
     * @return A writer for the file
     * @throws IOException When writing fails
     */
    public static Writer getWriter(String file) throws IOException {
        return getWriter(file, false);
    }

    /**
     * Gets a writer for the specified file
     *
     * @param file A file
     * @return A writer for the file
     * @throws IOException When writing fails
     */
    public static Writer getWriter(File file) throws IOException {
        return getWriter(file, false);
    }

    /**
     * Gets a writer for the specified file
     *
     * @param file   A file
     * @param append Whether to append to the file (do not overwrite)
     * @return A writer for the file
     * @throws IOException When writing fails
     */
    public static Writer getWriter(String file, boolean append) throws IOException {
        return new OutputStreamWriter(new FileOutputStream(file, append), CHARSET);
    }

    /**
     * Gets a writer for the specified file
     *
     * @param file   A file
     * @param append Whether to append to the file (do not overwrite)
     * @return A writer for the file
     * @throws IOException When writing fails
     */
    public static Writer getWriter(File file, boolean append) throws IOException {
        return new OutputStreamWriter(new FileOutputStream(file, append), CHARSET);
    }

    /**
     * Gets a reader for the specified file
     *
     * @param file A file
     * @return A reader for the file
     * @throws IOException When reading failed
     */
    public static Reader getReader(String file) throws IOException {
        return new InputStreamReader(new FileInputStream(file), CHARSET);
    }

    /**
     * Gets a reader for the specified file
     *
     * @param file A file
     * @return A reader for the file
     * @throws IOException When reading failed
     */
    public static Reader getReader(File file) throws IOException {
        return new InputStreamReader(new FileInputStream(file), CHARSET);
    }

    /**
     * Gets a reader for the specified file, automatically detecting encoding based on BOM
     *
     * @param file A file
     * @return A reader for the file
     * @throws IOException When reading failed
     */
    public static Reader getAutoReader(String file) throws IOException {
        return new AutoReader(new FileInputStream(file));
    }

    /**
     * Gets a reader for the specified file, automatically detecting encoding based on BOM
     *
     * @param file A file
     * @return A reader for the file
     * @throws IOException When reading failed
     */
    public static Reader getAutoReader(File file) throws IOException {
        return new AutoReader(new FileInputStream(file));
    }

    /**
     * Reads the complete content of the specified stream with the specified charset
     *
     * @param stream  A stream
     * @param charset The charset to use
     * @return The stream's content
     * @throws IOException when reading fails
     */
    public static String read(InputStream stream, Charset charset) throws IOException {
        return read(new InputStreamReader(stream, charset));
    }

    /**
     * Reads the complete content of the specified reader
     *
     * @param reader A reader
     * @return The reader's content
     * @throws IOException when reading fails
     */
    public static String read(Reader reader) throws IOException {
        StringBuilder fileData = new StringBuilder(1000);
        char[] buf = new char[BUFFER_SIZE];
        int numRead;
        while ((numRead = reader.read(buf)) != -1) {
            fileData.append(buf, 0, numRead);
        }
        reader.close();
        return fileData.toString();
    }

    /**
     * Loads all the content from the specified input stream
     *
     * @param stream The stream to load from
     * @return The loaded content
     * @throws IOException When the reading the stream fails
     */
    public static byte[] load(InputStream stream) throws IOException {
        List<byte[]> content = new ArrayList<>();
        byte[] buffer = new byte[BUFFER_SIZE];
        int length = 0;
        int read;
        int size = 0;
        while (true) {
            read = stream.read(buffer, length, BUFFER_SIZE - length);
            if (read == -1) {
                if (length != 0) {
                    content.add(buffer);
                    size += length;
                }
                break;
            }
            length += read;
            if (length == BUFFER_SIZE) {
                content.add(buffer);
                size += BUFFER_SIZE;
                buffer = new byte[BUFFER_SIZE];
                length = 0;
            }
        }

        byte[] result = new byte[size];
        int current = 0;
        for (int i = 0; i != content.size(); i++) {
            if (i == content.size() - 1) {
                // the last buffer
                System.arraycopy(content.get(i), 0, result, current, size - current);
            } else {
                System.arraycopy(content.get(i), 0, result, current, BUFFER_SIZE);
                current += BUFFER_SIZE;
            }
        }
        return result;
    }

    /**
     * Deletes a folder
     *
     * @param folder The folder to delete
     * @return true if the operation succeeded, false otherwise
     */
    public static boolean deleteFolder(File folder) {
        boolean success = true;
        File[] children = folder.listFiles();
        if (children == null)
            return false;
        for (int i = 0; i != children.length; i++) {
            if (children[i].isFile())
                success &= children[i].delete();
            else
                success &= deleteFolder(children[i]);
        }
        return success && folder.delete();
    }
}
