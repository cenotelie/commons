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

package fr.cenotelie.commons.lsp.structures;

/**
 * The different kinds of symbols
 *
 * @author Laurent Wouters
 */
public interface SymbolKind {
    int FILE = 1;
    int MODULE = 2;
    int NAMESPACE = 3;
    int PACKAGE = 4;
    int CLASS = 5;
    int METHOD = 6;
    int PROPERTY = 7;
    int FIELD = 8;
    int CONSTRUCTOR = 9;
    int ENUM = 10;
    int INTERFACE = 11;
    int FUNCTION = 12;
    int VARIABLE = 13;
    int CONSTANT = 14;
    int STRING = 15;
    int NUMBER = 16;
    int BOOLEAN = 17;
    int ARRAY = 18;
}
