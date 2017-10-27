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
import java.nio.charset.Charset;

/**
 * Implements an input stream that automatically detects the text encoding based on the presence of a BOM.
 *
 * @author Laurent Wouters
 */
public class AutoInputStream extends InputStream {
    /**
     * The byte-order mark for UTF8
     */
    private static final int[] BOM_UTF8 = new int[]{0xEF, 0xBB, 0xBF};
    /**
     * The byte-order mark for UTF16 little endian
     */
    private static final int[] BOM_UTF16_LE = new int[]{0xFF, 0xFE};
    /**
     * The byte-order mark for UTF16 big endian
     */
    private static final int[] BOM_UTF16_BE = new int[]{0xFE, 0xFF};
    /**
     * The byte-order mark for UTF32 little endian
     */
    private static final int[] BOM_UTF32_LE = new int[]{0xFF, 0xFE, 0x00, 0x00};
    /**
     * The byte-order mark for UTF32 big endian
     */
    private static final int[] BOM_UTF32_BE = new int[]{0x00, 0x00, 0xFE, 0xFF};

    /**
     * The original input stream
     */
    private final InputStream stream;
    /**
     * The detected charset
     */
    private Charset charset;
    /**
     * The prefix to read before the stream
     */
    private int[] prefix;
    /**
     * The index of the next character to read
     */
    private int index;

    /**
     * Initializes this stream
     *
     * @param stream The input steam
     */
    public AutoInputStream(InputStream stream) {
        this.stream = stream;
        this.charset = null;
        this.prefix = null;
        this.index = -1;
    }

    /**
     * Gets the charset for this stream
     *
     * @return The charset
     * @throws IOException When reading fails
     */
    public Charset getCharset() throws IOException {
        if (index < 0)
            doDetectEncoding();
        return charset;
    }

    @Override
    public int read() throws IOException {
        if (index < 0)
            doDetectEncoding();
        if (prefix != null && index < prefix.length)
            return prefix[index++];
        return stream.read();
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }

    /**
     * Setups the stream header
     *
     * @param charset The detected charset
     * @param prefix  The prefix for the stream
     */
    private void doSetup(Charset charset, int[] prefix) {
        this.charset = charset;
        this.prefix = prefix;
        this.index = 0;
    }

    /**
     * Detects the encoding of the steam
     *
     * @throws IOException When reading fails
     */
    private void doDetectEncoding() throws IOException {
        int b0 = stream.read();
        if (b0 == -1) {
            // steam was empty
            doSetup(IOUtils.CHARSET, null);
            return;
        }
        int b1 = stream.read();
        if (b1 == -1) {
            // stream contained just one byte
            doSetup(IOUtils.CHARSET, new int[]{b1});
            return;
        }

        if (b0 == BOM_UTF8[0] && b1 == BOM_UTF8[1]) {
            // starting as UTF-8 BOM
            int b2 = stream.read();
            if (b2 == -1) {
                // weird, the stream ended here, assume that was not BOM
                doSetup(IOUtils.CHARSET, new int[]{b0, b1});
                return;
            }
            if (b2 == BOM_UTF8[2]) {
                // this is the UTF-8 BOM
                doSetup(IOUtils.UTF8, null);
                return;
            }
            // starts as UTF-8 BOM, but was not, assume that was not BOM
            doSetup(IOUtils.CHARSET, new int[]{b0, b1, b2});
            return;
        }

        if (b0 == BOM_UTF16_BE[0] && b1 == BOM_UTF16_BE[1]) {
            // matched the UTF-16 BE BOM
            doSetup(Charset.forName("UTF-16BE"), null);
            return;
        }

        if (b0 == BOM_UTF32_BE[0] && b1 == BOM_UTF32_BE[1]) {
            // starting as a UTF-32 BE BOM
            int b2 = stream.read();
            if (b2 == -1) {
                // weird, the stream ended here, assume that was not BOM
                doSetup(IOUtils.CHARSET, new int[]{b0, b1});
                return;
            }
            if (b2 != BOM_UTF32_BE[2]) {
                // starts as UTF-32 BE, but was not, assume that was not BOM
                doSetup(IOUtils.CHARSET, new int[]{b0, b1, b2});
                return;
            }
            int b3 = stream.read();
            if (b3 == -1) {
                // weird, the stream ended here, assume that was not BOM
                doSetup(IOUtils.CHARSET, new int[]{b0, b1, b2});
                return;
            }
            if (b3 != BOM_UTF32_BE[3]) {
                // starts as UTF-32 BE, but was not, assume that was not BOM
                doSetup(IOUtils.CHARSET, new int[]{b0, b1, b2, b3});
                return;
            }
            // matched the UTF-32 BE BOM
            doSetup(Charset.forName("UTF-32BE"), null);
            return;
        }

        if (b0 != BOM_UTF16_LE[0] || b1 != BOM_UTF16_LE[1]) {
            // not a BOM
            doSetup(IOUtils.CHARSET, new int[]{b0, b1});
            return;
        }
        // starts as UTF-16 LE BOM
        int b2 = stream.read();
        if (b2 == -1) {
            // was a UTF-16 LE BOM alone
            doSetup(Charset.forName("UTF-16LE"), null);
            return;
        }
        if (b2 != BOM_UTF32_LE[2]) {
            // not continuing as a UTF-32 LE BOM
            doSetup(Charset.forName("UTF-16LE"), new int[]{b2});
            return;
        }
        int b3 = stream.read();
        if (b3 == -1) {
            // was a UTF-16 LE BOM followed by b2
            doSetup(Charset.forName("UTF-16LE"), new int[]{b2});
            return;
        }
        if (b3 != BOM_UTF32_LE[3]) {
            // was a UTF-16 LE BOM followed by b2 and b3
            doSetup(Charset.forName("UTF-16LE"), new int[]{b2, b3});
            return;
        }
        // is a UTF-32 LE BOM
        doSetup(Charset.forName("UTF-32LE"), null);
    }
}
