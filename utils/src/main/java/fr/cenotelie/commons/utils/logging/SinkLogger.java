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
 * Represents a logger that does nothing
 *
 * @author Laurent Wouters
 */
public class SinkLogger implements Logger {
    /**
     * Flags whether the logger received error
     */
    private boolean onError;

    /**
     * Gets whether this logger received errors since its last reset
     *
     * @return true if errors were received
     */
    public boolean isOnError() {
        return onError;
    }

    /**
     * Resets this logger error flag
     */
    public void reset() {
        onError = false;
    }

    @Override
    public void debug(Object message) {
    }

    @Override
    public void info(Object message) {
    }

    @Override
    public void warning(Object message) {
    }

    @Override
    public void error(Object message) {
        onError = true;
    }
}