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

import fr.cenotelie.commons.storage.StorageAccess;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represents a series of modifications that occurred to a page when writing
 *
 * @author Laurent Wouters
 */
class PageEdits {
    /**
     * The initial size of the edits buffers
     */
    private static final int BUFFER_SIZE = 8;

    /**
     * The starting indices of the stored edits
     */
    private int[] editIndex;
    /**
     * The content of the stored edits
     */
    private byte[][] editContent;
    /**
     * The number of edits
     */
    private int editCount;

    /**
     * Initializes this structure as empty
     */
    public PageEdits() {
        this.editIndex = new int[BUFFER_SIZE];
        this.editContent = new byte[BUFFER_SIZE][];
        this.editCount = 0;
    }

    /**
     * Initializes this structure by reading its content from the specified access
     *
     * @param access The access to use
     */
    public PageEdits(StorageAccess access) {
        this.editCount = access.readInt();
        this.editIndex = new int[editCount];
        this.editContent = new byte[editCount][];
        for (int i = 0; i != editCount; i++) {
            editIndex[i] = access.readInt();
            editContent[i] = access.readBytes(editIndex[i]);
        }
    }

    /**
     * Gets the number of edits
     *
     * @return The number of edits
     */
    public int getEditCount() {
        return editCount;
    }

    /**
     * Pushes a new edit
     *
     * @param index   The starting index of this edit
     * @param content The content of this edit
     */
    public void push(int index, byte[] content) {
        if (editCount == editIndex.length) {
            editIndex = Arrays.copyOf(editIndex, editIndex.length * 2);
            editContent = Arrays.copyOf(editContent, editContent.length * 2);
        }
        editIndex[editCount] = index;
        editContent[editCount] = content;
        editCount++;
    }

    /**
     * Applies this collection of edits to the byte buffer of a page
     *
     * @param buffer The byte buffer of a page
     */
    public void applyTo(ByteBuffer buffer) {
        for (int i = 0; i != editCount; i++) {
            buffer.position(editIndex[i]);
            buffer.put(editContent[i], 0, editContent[i].length);
        }
    }

    /**
     * Compacts the edits contained in this page
     */
    public void compact() {
        if (editCount <= 1)
            return;
        int[] fragmentBegin = new int[BUFFER_SIZE];
        int[] fragmentLength = new int[BUFFER_SIZE];
        int fragmentCount = 0;
        boolean compacted = false;

        for (int i = 0; i != editCount; i++) {
            int start = editIndex[i];
            int length = editContent[i].length;
            boolean handled = false;
            for (int j = 0; j != fragmentCount; j++) {
                if (start > fragmentBegin[j] + fragmentLength[j])
                    // this edit is strictly after the current fragment (not adjacent)
                    continue;
                if (start == fragmentBegin[j] + fragmentLength[j]) {
                    // this edit is right after the current fragment => extend this fragment
                    // TODO: continue here

                    compacted = true;
                    handled = true;
                    break;
                }
                if (fragmentBegin[j] > start + length) {
                    // this edit is completely before the current fragment, we must insert it here
                    if (fragmentCount == fragmentBegin.length) {
                        fragmentBegin = Arrays.copyOf(fragmentBegin, fragmentBegin.length * 2);
                        fragmentLength = Arrays.copyOf(fragmentLength, fragmentLength.length * 2);
                    }
                    // shift the fragments to the right
                    for (int k = fragmentCount - 1; k != j - 1; k--) {
                        fragmentBegin[k + 1] = fragmentBegin[k];
                        fragmentLength[k + 1] = fragmentLength[k];
                    }
                    // insert the edit as a new fragment here
                    fragmentBegin[j] = start;
                    fragmentLength[j] = length;
                    fragmentCount++;
                    handled = true;
                    break;
                }
                if (fragmentBegin[j] <= start + length) {
                    // this edit is just before (adjacent) to the current fragment, or the current fragment starts within the edit
                    // since we are here this edit is guaranteed to be strictly after the preceding fragment, if any
                    // so we can extend the current fragment to the left
                    fragmentBegin[j] = start;
                    // then check whether to extend this fragment to the right
                    int endFragment = fragmentBegin[j] + fragmentLength[j];
                    int endEdit = start + length;
                    if (endEdit > endFragment) {
                        // the edit ends after the modified fragment
                        // first, the new length of the fragment is the length of the edit
                        fragmentLength[j] = length;
                        // then, look for candidate fragments to be merged within this one
                        for (int k = j + 1; k != fragmentCount; k++) {
                            if (fragmentBegin[k] > fragmentBegin[j] + fragmentLength[j])
                                // strictly after (not adjacent)
                                break;
                            // merge the fragment k
                            // TODO: continue here
                        }
                    } else {
                        // the edit ends within, or just before the modified fragment
                        fragmentLength[j] = endFragment - start;
                    }
                    handled = true;
                    compacted = true;
                    break;
                }
            }
            if (!handled) {
                // add a new fragment at the end
                if (fragmentCount == fragmentBegin.length) {
                    fragmentBegin = Arrays.copyOf(fragmentBegin, fragmentBegin.length * 2);
                    fragmentLength = Arrays.copyOf(fragmentLength, fragmentLength.length * 2);
                }
                fragmentBegin[fragmentCount] = start;
                fragmentLength[fragmentCount] = length;
                fragmentCount++;
            }
        }

        if (!compacted)
            // we achieved nothing ...
            return;

        // initializes the new edits
        byte[][] newEditContent = new byte[fragmentCount][];
        for (int i = 0; i != fragmentCount; i++)
            newEditContent[i] = new byte[fragmentLength[i]];
        // apply the current edits to the new ones
        for (int i = 0; i != editCount; i++) {
            for (int j = 0; j != fragmentCount; j++) {
                if (editIndex[i] > fragmentBegin[j] + fragmentLength[j])
                    // this edit is strictly after the current fragment (not adjacent)
                    continue;
                // we are within the current fragment by construction
                System.arraycopy(
                        editContent[i],
                        0,
                        newEditContent[j],
                        editIndex[i] - fragmentBegin[j],
                        editContent[i].length
                );
                break;
            }
        }
        // commit
        editIndex = fragmentBegin;
        editContent = newEditContent;
        editCount = fragmentCount;
    }

    /**
     * Gets the length of the serialized data for these edits
     *
     * @return The length of the serialized data
     */
    public int getSerializationLength() {
        int result = 4; // size for storing the number of edits
        for (int i = 0; i != editCount; i++) {
            result += editContent[i].length + 4; // for the index of the edit within the page
        }
        return result;
    }

    /**
     * Serializes the content of these edits
     *
     * @param access The access to use for the serialization
     */
    public void serialize(StorageAccess access) {
        access.writeInt(editCount);
        for (int i = 0; i != editCount; i++) {
            access.writeInt(editContent[i].length);
            access.writeBytes(editContent[i]);
        }
    }
}
