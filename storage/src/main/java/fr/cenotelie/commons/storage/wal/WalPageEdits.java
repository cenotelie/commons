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

package fr.cenotelie.commons.storage.wal;

import fr.cenotelie.commons.utils.ByteUtils;

import java.util.Arrays;

/**
 * Represents the changes made to a page
 *
 * @author Laurent Wouters
 */
class WalPageEdits {
    /**
     * The serialization size of the edits header:
     * - int32: the number of edits
     */
    public static final int SERIALIZATION_SIZE_HEADER = 4;
    /**
     * The serialization size of an edit's header:
     * - int32: offset from the beginning of the page
     * - int32: length of the edit in bytes
     */
    public static final int SERIALIZATION_SIZE_EDIT_HEADER = 4 + 4;

    /**
     * The initial size of the edits buffers
     */
    private static final int BUFFER_SIZE = 8;

    /**
     * The edits within this page, compacted as:
     * int32: index within this page
     * int32: length of the edit
     */
    protected long[] edits;
    /**
     * The number of edits
     */
    protected int editsCount;

    /**
     * Initializes this structure
     */
    public WalPageEdits() {
        this.edits = null;
        this.editsCount = 0;
    }

    /**
     * Registers a new edit made to this page
     *
     * @param index  The index of the edit in this page
     * @param length The length of the edit
     */
    public void addEdit(int index, int length) {
        if (edits == null)
            edits = new long[BUFFER_SIZE];
        for (int i = 0; i != editsCount; i++) {
            // examine the existing edits in order
            int cIndex = editIndex(edits[i]);
            int cLength = editLength(edits[i]);
            if (cIndex > index + length) {
                // strictly before the current edit => must insert here
                shiftEditsRight(i);
                edits[i] = edit(index, length);
                editsCount++;
                return;
            }
            if (index > cIndex + cLength)
                // strictly after the current edit => examine next
                continue;
            // the edits are adjacent or overlap
            // we are also guaranteed to not be adjacent to, or overlap with, an edit on the left
            // merge the current edit with the one to be inserted
            int newIndex = Math.min(index, cIndex); // new edit starts at the min of both starting points
            int furthest = Math.max(index + length, cIndex + cLength); // the furthest offset reached by the merged edits
            int newLength = furthest - newIndex;
            edits[i] = edit(newIndex, newLength);
            // now, we must check that the current edit does not encompass existing edits on the right
            mergeCandidatesWith(i);
            return;
        }
        // here the edit must be inserted at the end
        if (editsCount == edits.length)
            edits = Arrays.copyOf(edits, edits.length * 2);
        edits[editsCount] = edit(index, length);
        editsCount++;
    }

    /**
     * Shifts all the edits on the right, starting at the specified index
     *
     * @param from The starting index
     */
    private void shiftEditsRight(int from) {
        if (editsCount == edits.length)
            edits = Arrays.copyOf(edits, edits.length * 2);
        for (int i = editsCount; i != from; i--) {
            edits[i] = edits[i - 1];
        }
    }

    /**
     * Merges candidate edits on the right of the specified one
     *
     * @param from The index of the merge-able edit
     */
    private void mergeCandidatesWith(int from) {
        int fIndex = editIndex(edits[from]);
        int fEnd = fIndex + editLength(edits[from]);
        int merged = 0;
        for (int i = from + 1; i != editsCount; i++) {
            int cIndex = editIndex(edits[i]);
            int cLength = editLength(edits[i]);
            if (cIndex > fEnd)
                // the candidate is strictly after the merged, stop here
                break;
            // the candidate is either adjacent or overlap with the merging edit
            fEnd = Math.max(fEnd, cIndex + cLength); // the new end for the merge
            edits[from] = edit(fIndex, fEnd - fIndex);
            merged++;
        }
        if (merged == 0)
            return;
        // now we need to shift to the left remaining edits
        for (int i = from + 1; i != editsCount - merged; i++) {
            edits[i] = edits[i + merged];
        }
        editsCount -= merged;
    }

    /**
     * Gets whether this collection of edits intersect wth the edits of another collection
     *
     * @param data Another collection of edits
     * @return Whether there are conflicts
     */
    public boolean intersects(WalPageEdits data) {
        // initialize
        int i = 0;
        int j = 0;
        // while we did not reach the end of a collection of edits
        while (i < this.editsCount && j < data.editsCount) {
            // get the data
            int iStart = editIndex(this.edits[i]);
            int iEnd = iStart + editLength(this.edits[i]);
            int jStart = editIndex(data.edits[j]);
            int jEnd = iStart + editLength(data.edits[j]);
            // inspect the two edits
            if (iStart >= jEnd) {
                // the left side starts after the right side, advance on the right
                j++;
                continue;
            }
            if (jStart >= iEnd) {
                // the right side starts after the left side, advance on the left
                i++;
                continue;
            }
            // this looks like an overlap ...
            return true;
        }
        return false;
    }

    /**
     * Gets the serialization length of the edits
     *
     * @return The serialization length of the edits
     */
    public int getSerializationLength() {
        int result = SERIALIZATION_SIZE_HEADER;
        for (int i = 0; i != editsCount; i++) {
            result += SERIALIZATION_SIZE_EDIT_HEADER + editLength(edits[i]);
        }
        return result;
    }

    /**
     * Gets an edit's index
     *
     * @param edit An edit
     * @return The index of the edit
     */
    public static int editIndex(long edit) {
        return (int) (edit >>> 32);
    }

    /**
     * Gets an edit's length
     *
     * @param edit An edit
     * @return The length of the edit
     */
    public static int editLength(long edit) {
        return (int) (edit & 0xFFFFFFFFL);
    }

    /**
     * Gets an edit
     *
     * @param index  The index of the edit
     * @param length The length of the edit
     * @return The edit
     */
    private static long edit(int index, int length) {
        return (ByteUtils.uLong(index) << 32) | ByteUtils.uLong(length);
    }
}
