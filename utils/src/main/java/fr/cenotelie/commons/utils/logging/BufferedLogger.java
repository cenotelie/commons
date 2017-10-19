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

package fr.cenotelie.commons.utils.logging;

import fr.cenotelie.commons.utils.IOUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Logger that buffers its messages so that they can be inspected after the fact
 *
 * @author Laurent Wouters
 */
public class BufferedLogger implements Logger {
    /**
     * Messages at the debug level
     */
    private final List<Object> msgDebug;
    /**
     * Messages at the info level
     */
    private final List<Object> msgInfo;
    /**
     * Messages at the warning level
     */
    private final List<Object> msgWarning;
    /**
     * Messages at the error level
     */
    private final List<Object> msgError;

    /**
     * Initializes this logger
     */
    public BufferedLogger() {
        this.msgDebug = new ArrayList<>();
        this.msgInfo = new ArrayList<>();
        this.msgWarning = new ArrayList<>();
        this.msgError = new ArrayList<>();
    }

    /**
     * Clears all the messages store in this buffered logger
     */
    public void clear() {
        msgDebug.clear();
        msgInfo.clear();
        msgWarning.clear();
        msgError.clear();
    }

    /**
     * Gets the messages at the debug level
     *
     * @return The messages at the debug level
     */
    public List<Object> getDebugMessages() {
        return Collections.unmodifiableList(msgDebug);
    }

    /**
     * Gets the messages at the info level
     *
     * @return The messages at the info level
     */
    public List<Object> getInfoMessages() {
        return Collections.unmodifiableList(msgInfo);
    }

    /**
     * Gets the messages at the warning level
     *
     * @return The messages at the warning level
     */
    public List<Object> getWarningMessages() {
        return Collections.unmodifiableList(msgWarning);
    }

    /**
     * Gets the messages at the error level
     *
     * @return The messages at the error level
     */
    public List<Object> getErrorMessages() {
        return Collections.unmodifiableList(msgError);
    }

    @Override
    public void debug(Object message) {
        msgDebug.add(message);
    }

    @Override
    public void info(Object message) {
        msgInfo.add(message);
    }

    @Override
    public void warning(Object message) {
        msgWarning.add(message);
    }

    @Override
    public void error(Object message) {
        msgError.add(message);
    }

    /**
     * Gets all the errors in this log as a string
     *
     * @return The content of the log
     */
    public String getErrorsAsString() {
        StringBuilder builder = new StringBuilder();
        for (Object error : getErrorMessages()) {
            builder.append(error.toString());
            builder.append(IOUtils.LINE_SEPARATOR);
        }
        return builder.toString();
    }
}
