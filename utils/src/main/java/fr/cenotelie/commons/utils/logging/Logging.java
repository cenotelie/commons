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

/**
 * Logging management
 *
 * @author Laurent Wouters
 */
public class Logging {
    /**
     * The default logger
     */
    private static Logger DEFAULT = new ConsoleLogger();
    /**
     * The loggers for specific threads
     */
    private static final ThreadLocal<Logger> THREAD_LOGGERS = new ThreadLocal<>();

    /**
     * Gets the default logger
     *
     * @return The default logger
     */
    public static Logger getDefault() {
        return DEFAULT;
    }

    /**
     * Sets the default logger
     *
     * @param logger The default logger
     */
    public static void setDefault(Logger logger) {
        DEFAULT = logger;
    }

    /**
     * Gets the logger for the current thread
     *
     * @return The logger to use
     */
    public static Logger get() {
        Logger result = THREAD_LOGGERS.get();
        if (result != null)
            return result;
        return DEFAULT;
    }

    /**
     * Sets the logger for the current thread
     *
     * @param logger The logger to use
     */
    public static void set(Logger logger) {
        THREAD_LOGGERS.set(logger);
    }
}
