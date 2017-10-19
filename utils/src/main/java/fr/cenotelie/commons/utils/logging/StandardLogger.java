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

import java.util.logging.Level;

/**
 * A standard logger encapsulating a standard Java logger
 *
 * @author Laurent Wouters
 */
public class StandardLogger implements Logger {
    /**
     * The backend logger
     */
    private final java.util.logging.Logger inner;

    /**
     * Initializes this logger
     *
     * @param inner The backend logger to use
     */
    public StandardLogger(java.util.logging.Logger inner) {
        this.inner = inner;
    }

    @Override
    public void debug(Object message) {
        log(Level.FINE, message);
    }

    @Override
    public void info(Object message) {
        log(Level.INFO, message);
    }

    @Override
    public void warning(Object message) {
        log(Level.WARNING, message);
    }

    @Override
    public void error(Object message) {
        log(Level.SEVERE, message);
    }

    private void log(Level level, Object message) {
        if (message instanceof Throwable) {
            inner.log(level, message.toString(), (Throwable) message);
        } else {
            inner.log(level, message.toString());
        }
    }
}
