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

package fr.cenotelie.commons.lsp.engine;

import fr.cenotelie.commons.lsp.structures.TextEdit;
import fr.cenotelie.hime.redist.lexer.PrefetchedText;

import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;

/**
 * Implements the content of a document backed by a simple Java string
 *
 * @author Laurent Wouters
 */
public class DocumentContentString implements DocumentContent {
    /**
     * The data
     */
    private String data;

    /**
     * Initializes this content
     *
     * @param data The data
     */
    public DocumentContentString(String data) {
        this.data = data;
    }

    @Override
    public Reader getReader() {
        return new StringReader(data);
    }

    @Override
    public void applyEdits(TextEdit[] edits) {
        if (edits == null || edits.length == 0)
            return;
        Arrays.sort(edits, TextEdit.COMPARATOR_ORDER);
        PrefetchedText text = new PrefetchedText(data);
        StringBuilder builder = new StringBuilder();
        int current = 0; // the current index in the text
        for (int i = 0; i != edits.length; i++) {
            // gets the inclusive bounds of this edit
            int start = text.getLineIndex(edits[i].getRange().getStart().getLine() + 1) + edits[i].getRange().getStart().getCharacter();
            int end = text.getLineIndex(edits[i].getRange().getEnd().getLine() + 1) + edits[i].getRange().getEnd().getCharacter();
            if (start > current) {
                // this edit is strictly after the current index
                // put into the result the text between the current index and the start of the edit
                builder.append(text.getValue(current, start - current));
            }
            if (!edits[i].isDeletion()) {
                // not a deletion, this is either a replacement or an insert
                // => simply add the new text in the builder
                builder.append(edits[i].getNewText());
            }
            // in all other cases, drop the text with the span of this edit
            // the new current index is then just after the end of this edit
            current = end;
        }
        // adds the trailing text
        if (current < data.length())
            builder.append(data.substring(current));
        data = builder.toString();
    }

    @Override
    public DocumentContent cloneWith(TextEdit[] edits) {
        DocumentContentString newVersion = new DocumentContentString(data);
        newVersion.applyEdits(edits);
        return newVersion;
    }
}
