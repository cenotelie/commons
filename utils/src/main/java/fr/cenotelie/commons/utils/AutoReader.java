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

package fr.cenotelie.commons.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Implements an input stream reader that automatically detects the text encoding based on the presence of a BOM.
 *
 * @author Laurent Wouters
 */
public class AutoReader extends InputStreamReader {
    /**
     * Initializes this reader
     *
     * @param inputStream The original input stream
     * @throws IOException When reading fails
     */
    public AutoReader(InputStream inputStream) throws IOException {
        this(new AutoInputStream(inputStream));
    }

    /**
     * Initializes this reader
     *
     * @param autoInputStream The automatic input stream
     * @throws IOException When reading fails
     */
    private AutoReader(AutoInputStream autoInputStream) throws IOException {
        super(autoInputStream, autoInputStream.getCharset());
    }
}
