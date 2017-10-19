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
 * The kind of a completion entry
 *
 * @author Laurent Wouters
 */
public interface CompletionItemKind {
    int TEXT = 1;
    int METHOD = 2;
    int FUNCTION = 3;
    int CONSTRUCTOR = 4;
    int FIELD = 5;
    int VARIABLE = 6;
    int CLASS = 7;
    int INTERFACE = 8;
    int MODULE = 9;
    int PROPERTY = 10;
    int UNIT = 11;
    int VALUE = 12;
    int ENUM = 13;
    int KEYWORD = 14;
    int SNIPPET = 15;
    int COLOR = 16;
    int FILE = 17;
    int REFERENCE = 18;
}
