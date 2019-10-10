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

import java.io.IOException;
import java.io.Reader;

/**
 * Represents a stream of characters that can be rewound
 *
 * @author Laurent Wouters
 */
public class RewindableTextStream {
    /**
     * Size of the buffer used to read from the input
     */
    private static final int BUFFER_SIZE = 1024;
    /**
     * Size of the ring storing read characters that can be rewound
     */
    private static final int RING_SIZE = 1024;
    /**
     * Encapsulated text reader
     */
    private final Reader reader;
    /**
     * First stage buffer for batch reading of the stream
     */
    private final char[] buffer;
    /**
     * Ring memory of this reader storing the already read characters
     */
    private final char[] ring;
    /**
     * Index where the next character shall be read in the buffer
     */
    private int bufferStart;
    /**
     * Current length of the buffer
     */
    private int bufferLength;
    /**
     * Marker of the end of input
     */
    private boolean atEnd;
    /**
     * Start index of the ring where to read characters
     */
    private int ringStart;
    /**
     * Index for inserting new characters in the ring
     */
    private int ringNextEntry;

    /**
     * Initializes this stream
     *
     * @param reader The underlying text reader
     */
    public RewindableTextStream(Reader reader) {
        this.reader = reader;
        this.buffer = new char[BUFFER_SIZE];
        this.bufferStart = 0;
        this.bufferLength = 0;
        this.ring = new char[RING_SIZE];
        this.ringStart = 0;
        this.ringNextEntry = 0;
    }

    /**
     * Determines whether the end of the input has been reached
     *
     * @return <code>true</code> if the end of the input has been reached
     */
    public boolean isAtEnd() {
        return atEnd;
    }

    /**
     * Goes back into the stream of the given number of characters
     *
     * @param count The number of characters to rewind
     */
    public void rewind(int count) {
        ringStart -= count;
        if (ringStart < 0) {
            ringStart += RING_SIZE;
        }
    }

    /**
     * Reads the next character in the stream
     *
     * @return The next character
     */
    public char read() {
        if (atEnd) {
            return 0;
        }
        if (ringStart != ringNextEntry) {
            char value = ring[ringStart++];
            if (ringStart == RING_SIZE) {
                ringStart = 0;
            }
            return value;
        }
        return readBuffer();
    }

    /**
     * Reads the next character from the input
     *
     * @return The next character in the stream
     */
    private char readBuffer() {
        if (bufferStart == bufferLength) {
            bufferLength = -1;
            try {
                bufferLength = reader.read(buffer, 0, BUFFER_SIZE);
            } catch (IOException e) {
                // nothing to report
            }
            bufferStart = 0;
            if (bufferLength <= 0) {
                atEnd = true;
                return 0;
            }
        }
        atEnd = false;
        char c = buffer[bufferStart++];
        ring[ringNextEntry++] = c;
        if (ringNextEntry == RING_SIZE) {
            ringNextEntry = 0;
        }
        ringStart = ringNextEntry;
        return c;
    }
}
