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

package fr.cenotelie.commons.utils.ini;

import fr.cenotelie.commons.utils.IOUtils;
import fr.cenotelie.commons.utils.TextUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Represents an INI document with values associated to properties
 * This structure is NOT thread safe
 *
 * @author Laurent Wouters
 */
public class IniDocument {
    /**
     * The initial length of the buffer for loading a document
     */
    private static final int BUFFER_LENGTH = 1024;
    /**
     * The initial state of the loader state machine
     */
    private static final int STATE_INIT = 0x00;
    /**
     * The state when within a comment
     */
    private static final int STATE_COMMENT = 0x01;
    /**
     * The state when within the title of a section
     */
    private static final int STATE_SECTION_TITLE_WITHIN = 0x10;
    /**
     * The state when after the title of a section (still on the same line)
     */
    private static final int STATE_SECTION_TITLE_AFTER = 0x11;
    /**
     * The state when reading a property name
     */
    private static final int STATE_PROPERTY_NAME = 0x20;
    /**
     * The state when reading a property value
     */
    private static final int STATE_PROPERTY_VALUE = 0x21;
    /**
     * The global section of this document
     */
    private final IniSection global;
    /**
     * The named sections
     */
    private final Map<String, IniSection> sections;

    /**
     * Initializes an empty document
     */
    public IniDocument() {
        global = new IniSection(null);
        sections = new HashMap<>();
    }

    /**
     * Gets whether the specified character starts a comment in a document file
     *
     * @param c The character
     * @return Whether the specified character starts a comment in a document file
     */
    private static boolean isCommentStart(int c) {
        return c == '#' || c == ';';
    }

    /**
     * Gets whether the specified character is a line-ending character
     *
     * @param c The character
     * @return Whether the specified character is a line-ending character
     */
    private static boolean isLineEnding(int c) {
        return c == '\n' || c == '\r' || c == 0x2028 || c == 0x2029;
    }

    /**
     * Gets whether the character is a spacing character
     *
     * @param c The character
     * @return Whether the character is a spacing character
     */
    private static boolean isWhitespace(int c) {
        return c == 0x0020 || c == 0x0009 || c == 0x000B || c == 0x000C;
    }

    /**
     * Gets all the named sections in this document
     *
     * @return The named sections
     */
    public Collection<IniSection> getSections() {
        return new ArrayList<>(sections.values());
    }

    /**
     * Gets the global section in this document
     *
     * @return The global section in this document
     */
    public IniSection getGlobalSection() {
        return global;
    }

    /**
     * Gets the section with the specified name
     *
     * @param name A section's name
     * @return The section with the specified name
     */
    public IniSection getSection(String name) {
        if (name == null)
            return global;
        if (!sections.containsKey(name))
            sections.put(name, new IniSection(name));
        return sections.get(name);
    }

    /**
     * Gets a list of the values for the specified property in the global section
     *
     * @param property A property in the global section
     * @return A list of the values associated to the specified property
     */
    public List<String> getAll(String property) {
        return global.getAll(property);
    }

    /**
     * Gets the first value for the specified property in the global section
     *
     * @param property A property in the global section
     * @return The first value associated to the specified property
     */
    public String get(String property) {
        return global.get(property);
    }

    /**
     * Gets the list of values for the specified section and property
     *
     * @param section  A section in this document
     * @param property A property in the section
     * @return A list of the values associated to the specified property
     */
    public List<String> getAll(String section, String property) {
        if (section == null)
            return global.getAll(property);
        IniSection current = sections.get(section);
        if (current == null)
            return Collections.emptyList();
        return current.getAll(property);
    }

    /**
     * Gets the list of values for the specified section and property
     *
     * @param section  A section in this document
     * @param property A property in the section
     * @return A list of the values associated to the specified property
     */
    public String get(String section, String property) {
        if (section == null)
            return global.get(property);
        IniSection current = sections.get(section);
        if (current == null)
            return null;
        return current.get(property);
    }

    /**
     * Gets whether the specified property and associated value are present in a section
     *
     * @param section  A section
     * @param property A property
     * @param value    An associated value
     * @return Whether the property has the associated value
     */
    public boolean hasValue(String section, String property, String value) {
        if (section == null)
            return global.hasValue(property, value);
        IniSection current = sections.get(section);
        return current != null && current.hasValue(property, value);
    }

    /**
     * Adds the specified property-value pair in the specified section
     *
     * @param property A property in the section
     * @param value    The value to associate
     */
    public void add(String property, String value) {
        global.add(property, value);
    }

    /**
     * Adds the specified property-value pair in the specified section
     *
     * @param section  A section in this document
     * @param property A property in the section
     * @param value    The value to associate
     */
    public void add(String section, String property, String value) {
        getSection(section).add(property, value);
    }

    /**
     * Removes the specified property - value pair from global section
     *
     * @param property A property
     * @param value    The associated value to remove
     */
    public void remove(String property, String value) {
        global.remove(property, value);
    }

    /**
     * Removes the specified property - value pair from the specified section
     *
     * @param section  Name of the section in the document file
     * @param property A property
     * @param value    The associated value to remove
     */
    public void remove(String section, String property, String value) {
        getSection(section).remove(property, value);
    }

    /**
     * Sets the property, removing all previous values, if any
     *
     * @param property A property
     * @param value    The new value
     */
    public void set(String property, String value) {
        global.set(property, value);
    }

    /**
     * Sets the property, removing all previous values, if any
     *
     * @param section  A section in this document
     * @param property A property
     * @param value    The new value
     */
    public void set(String section, String property, String value) {
        getSection(section).set(property, value);
    }

    /**
     * Clears any value for the property
     *
     * @param property The property to clear
     */
    public void clear(String property) {
        global.clear(property);
    }

    /**
     * Clears any value for the property
     *
     * @param section  A section in this document
     * @param property The property to clear
     */
    public void clear(String section, String property) {
        getSection(section).clear(property);
    }

    /**
     * Exports this document to the specified file
     *
     * @param file The file to export to
     * @throws IOException When writing fails
     */
    public void save(String file) throws IOException {
        try (Writer writer = IOUtils.getWriter(file)) {
            save(writer);
        }
    }

    /**
     * Exports this document to the specified file
     *
     * @param file The file to export to
     * @throws IOException When writing fails
     */
    public void save(File file) throws IOException {
        try (Writer writer = IOUtils.getWriter(file)) {
            save(writer);
        }
    }

    /**
     * Exports this document to the specified writer
     *
     * @param writer The writer to use
     * @throws IOException When writing fails
     */
    private void save(Writer writer) throws IOException {
        global.save(writer);
        boolean before = !global.isEmpty();
        for (IniSection section : sections.values()) {
            if (before)
                writer.write(IOUtils.LINE_SEPARATOR);
            section.save(writer);
            before = (before || !section.isEmpty());
        }
        writer.flush();
    }

    /**
     * Imports the document from the specified file
     *
     * @param file The file to import from
     * @throws IOException When reading fails
     */
    public void load(String file) throws IOException {
        try (Reader reader = IOUtils.getReader(file)) {
            load(reader);
        }
    }

    /**
     * Imports the document from the specified file
     *
     * @param file The file to import from
     * @throws IOException When reading fails
     */
    public void load(File file) throws IOException {
        try (Reader reader = IOUtils.getReader(file)) {
            load(reader);
        }
    }

    /**
     * Imports the document from the specified stream
     *
     * @param stream  The stream to read from
     * @param charset The charset to use
     * @throws IOException when reading fails
     */
    public void load(InputStream stream, Charset charset) throws IOException {
        load(new InputStreamReader(stream, charset));
    }

    /**
     * Imports the document from the specified reader
     *
     * @param reader The reader
     * @throws IOException When reading fails
     */
    public void load(Reader reader) throws IOException {
        char[] buffer = new char[BUFFER_LENGTH];
        int bufferNext = 0;
        int state = STATE_INIT;
        String currentSection = null;
        String currentProperty = null;

        int c = reader.read();
        while (c > 0) {
            switch (state) {
                case STATE_INIT: {
                    if (isLineEnding(c)) {
                        state = STATE_INIT;
                    } else if (isWhitespace(c)) {
                        state = STATE_INIT;
                    } else if (isCommentStart(c)) {
                        state = STATE_COMMENT;
                    } else if (c == '[') {
                        bufferNext = 0;
                        state = STATE_SECTION_TITLE_WITHIN;
                    } else {
                        buffer[0] = (char) c;
                        bufferNext = 1;
                        state = STATE_PROPERTY_NAME;
                    }
                    break;
                }
                case STATE_COMMENT: {
                    if (isLineEnding(c))
                        state = STATE_INIT;
                    break;
                }
                case STATE_SECTION_TITLE_WITHIN: {
                    if (c == ']') {
                        currentSection = new String(buffer, 0, bufferNext);
                        currentSection = TextUtils.unescape(currentSection.trim());
                        state = STATE_SECTION_TITLE_AFTER;
                    } else {
                        if (bufferNext == buffer.length)
                            buffer = Arrays.copyOf(buffer, buffer.length + BUFFER_LENGTH);
                        buffer[bufferNext] = (char) c;
                        bufferNext++;
                        state = STATE_SECTION_TITLE_WITHIN;
                    }
                    break;
                }
                case STATE_SECTION_TITLE_AFTER: {
                    if (isLineEnding(c)) {
                        state = STATE_INIT;
                    } else if (isCommentStart(c)) {
                        state = STATE_COMMENT;
                    }
                    // any content after the section title on the same line is dropped
                    break;
                }
                case STATE_PROPERTY_NAME: {
                    if (isLineEnding(c)) {
                        // drop this content
                        state = STATE_INIT;
                    } else if (c == '=') {
                        currentProperty = new String(buffer, 0, bufferNext);
                        currentProperty = TextUtils.unescape(currentProperty.trim());
                        bufferNext = 0;
                        state = STATE_PROPERTY_VALUE;
                    } else {
                        if (bufferNext == buffer.length)
                            buffer = Arrays.copyOf(buffer, buffer.length + BUFFER_LENGTH);
                        buffer[bufferNext] = (char) c;
                        bufferNext++;
                        state = STATE_PROPERTY_NAME;
                    }
                    break;
                }
                case STATE_PROPERTY_VALUE: {
                    if (isLineEnding(c)) {
                        String value = new String(buffer, 0, bufferNext);
                        value = TextUtils.unescape(value.trim());
                        add(currentSection, currentProperty, value);
                        state = STATE_INIT;
                    } else {
                        if (bufferNext == buffer.length)
                            buffer = Arrays.copyOf(buffer, buffer.length + BUFFER_LENGTH);
                        buffer[bufferNext] = (char) c;
                        bufferNext++;
                        state = STATE_PROPERTY_VALUE;
                    }
                    break;
                }
            }
            c = reader.read();
        }
        if (state == STATE_PROPERTY_VALUE) {
            // was reading a value when the stream ended
            String value = new String(buffer, 0, bufferNext);
            value = TextUtils.unescape(value.trim());
            add(currentSection, currentProperty, value);
        }
    }
}
