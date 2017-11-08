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

package fr.cenotelie.commons.storage.raw;

import fr.cenotelie.commons.storage.IOAccess;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

/**
 * Tests the FileBackend class
 *
 * @author Laurent Wouters
 */
public class RawFileBufferedTest {
    @Test
    public void testGetSizeEmpty() throws IOException {
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(File.createTempFile("test", ".bin"), true))) {
            Assert.assertEquals("Unexpected length", 0, rawFile.getSize());
        }
    }

    @Test
    public void testGetSizeSingleBlock() throws IOException {
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(File.createTempFile("test", ".bin"), true))) {
            try (IOAccess access = rawFile.access(0, 1, true)) {
                access.writeByte((byte) 5);
            }
            Assert.assertEquals("Unexpected length", RawFileBlock.BLOCK_SIZE, rawFile.getSize());
        }
    }

    @Test
    public void testGetSizeDoubleBlock() throws IOException {
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(File.createTempFile("test", ".bin"), true))) {
            try (IOAccess access = rawFile.access(0, 1, true)) {
                access.writeByte((byte) 5);
            }
            try (IOAccess access = rawFile.access(RawFileBlock.BLOCK_SIZE, 1, true)) {
                access.writeByte((byte) 6);
            }
            Assert.assertEquals("Unexpected length", RawFileBlock.BLOCK_SIZE * 2, rawFile.getSize());
        }
    }

    @Test
    public void testGetSizeAfterReload() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 1, true)) {
                access.writeByte((byte) 5);
            }
            rawFile.flush();
        }

        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, false))) {
            Assert.assertEquals("Unexpected length", RawFileBlock.BLOCK_SIZE, rawFile.getSize());
        }
    }

    @Test
    public void testGetSizeAfterReloadEmpty() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            rawFile.flush();
        }

        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, false))) {
            Assert.assertEquals("Unexpected length", 0, rawFile.getSize());
        }
    }


    @Test
    public void testGetIndexOnCreation() throws IOException {
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(File.createTempFile("test", ".bin"), true))) {
            try (IOAccess access = rawFile.access(0, 1, true)) {
                Assert.assertEquals("Unexpected index", 0, access.getIndex());
            }
        }
    }

    @Test
    public void testGetIndexAfterSeek() throws IOException {
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(File.createTempFile("test", ".bin"), true))) {
            try (IOAccess access = rawFile.access(0, 1, true)) {
                access.seek(RawFileBlock.BLOCK_SIZE + 4);
                Assert.assertEquals("Unexpected index", RawFileBlock.BLOCK_SIZE + 4, access.getIndex());
            }
        }
    }

    @Test
    public void testGetIndexAfterWriteByte() throws IOException {
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(File.createTempFile("test", ".bin"), true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.seek(4);
                access.writeByte((byte) 5);
                Assert.assertEquals("Unexpected index", 5, access.getIndex());
            }
        }
    }

    @Test
    public void testGetIndexAfterWriteBytes() throws IOException {
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(File.createTempFile("test", ".bin"), true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.seek(4);
                access.writeBytes(new byte[]{0x5, 0x6, 0x7});
                Assert.assertEquals("Unexpected index", 7, access.getIndex());
            }
        }
    }

    @Test
    public void testGetIndexAfterWriteChar() throws IOException {
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(File.createTempFile("test", ".bin"), true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.seek(4);
                access.writeChar('a');
                Assert.assertEquals("Unexpected index", 6, access.getIndex());
            }
        }
    }

    @Test
    public void testGetIndexAfterWriteInt() throws IOException {
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(File.createTempFile("test", ".bin"), true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.seek(4);
                access.writeInt(55);
                Assert.assertEquals("Unexpected index", 8, access.getIndex());
            }
        }
    }

    @Test
    public void testGetIndexAfterWriteLong() throws IOException {
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(File.createTempFile("test", ".bin"), true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.seek(4);
                access.writeLong(0x00BB00AA00FF00EEL);
                Assert.assertEquals("Unexpected index", 12, access.getIndex());
            }
        }
    }

    @Test
    public void testGetIndexAfterWriteFloat() throws IOException {
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(File.createTempFile("test", ".bin"), true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.seek(4);
                access.writeFloat(5.5f);
                Assert.assertEquals("Unexpected index", 8, access.getIndex());
            }
        }
    }

    @Test
    public void testGetIndexAfterWriteDouble() throws IOException {
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(File.createTempFile("test", ".bin"), true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.seek(4);
                access.writeDouble(5.5f);
                Assert.assertEquals("Unexpected index", 12, access.getIndex());
            }
        }
    }

    @Test
    public void testWriteSimpleByte() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 1, true)) {
                access.writeByte((byte) 5);
            }
            rawFile.flush();
        }
        Assert.assertTrue("FileBackend has not been created", file.exists());
        Assert.assertEquals("Unexpected file length", (long) RawFileBlock.BLOCK_SIZE, file.length());
        byte[] content = Files.readAllBytes(file.toPath());
        Assert.assertEquals("Unexpected content length", RawFileBlock.BLOCK_SIZE, content.length);
        Assert.assertEquals("Unexpected content", 5, content[0]);
    }

    @Test
    public void testReadSimpleByte() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.writeByte((byte) 5);
            }
            rawFile.flush();
        }

        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, false)) {
                Assert.assertEquals("Unexpected content", 5, access.readByte());
            }
        }
    }

    @Test
    public void testWriteSimpleBytes() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.writeBytes(new byte[]{0x5, 0x6, 0x7});
            }
            rawFile.flush();
        }
        Assert.assertTrue("FileBackend has not been created", file.exists());
        Assert.assertEquals("Unexpected file length", (long) RawFileBlock.BLOCK_SIZE, file.length());
        byte[] content = Files.readAllBytes(file.toPath());
        Assert.assertEquals("Unexpected content length", RawFileBlock.BLOCK_SIZE, content.length);
        Assert.assertEquals("Unexpected content", 5, content[0]);
        Assert.assertEquals("Unexpected content", 6, content[1]);
        Assert.assertEquals("Unexpected content", 7, content[2]);
    }

    @Test
    public void testReadSimpleBytes() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.writeBytes(new byte[]{0x5, 0x6, 0x7});
            }
            rawFile.flush();
        }

        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                Assert.assertEquals("Unexpected content", 5, access.readByte());
                Assert.assertEquals("Unexpected content", 6, access.readByte());
                Assert.assertEquals("Unexpected content", 7, access.readByte());
            }
        }
    }

    @Test
    public void testWriteSimpleChar() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.writeChar((char) 0xBBCC);
            }
            rawFile.flush();
        }
        Assert.assertTrue("FileBackend has not been created", file.exists());
        Assert.assertEquals("Unexpected file length", (long) RawFileBlock.BLOCK_SIZE, file.length());
        byte[] content = Files.readAllBytes(file.toPath());
        Assert.assertEquals("Unexpected content length", RawFileBlock.BLOCK_SIZE, content.length);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        Assert.assertEquals("Unexpected content", (char) 0xBBCC, buffer.getChar());
    }

    @Test
    public void testReadSimpleChar() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.writeChar((char) 0xBBCC);
            }
            rawFile.flush();
        }

        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, false)) {
                Assert.assertEquals("Unexpected content", (char) 0xBBCC, access.readChar());
            }
        }
    }

    @Test
    public void testWriteSimpleInt() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.writeInt(55);
            }
            rawFile.flush();
        }
        Assert.assertTrue("FileBackend has not been created", file.exists());
        Assert.assertEquals("Unexpected file length", (long) RawFileBlock.BLOCK_SIZE, file.length());
        byte[] content = Files.readAllBytes(file.toPath());
        Assert.assertEquals("Unexpected content length", RawFileBlock.BLOCK_SIZE, content.length);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        Assert.assertEquals("Unexpected content", 55, buffer.getInt());
    }

    @Test
    public void testReadSimpleInt() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.writeInt(55);
            }
            rawFile.flush();
        }

        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, false)) {
                Assert.assertEquals("Unexpected content", 55, access.readInt());
            }
        }
    }

    @Test
    public void testWriteSimpleLong() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.writeLong(0x00BB00AA00FF00EEL);
            }
            rawFile.flush();
        }
        Assert.assertTrue("FileBackend has not been created", file.exists());
        Assert.assertEquals("Unexpected file length", (long) RawFileBlock.BLOCK_SIZE, file.length());
        byte[] content = Files.readAllBytes(file.toPath());
        Assert.assertEquals("Unexpected content length", RawFileBlock.BLOCK_SIZE, content.length);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        Assert.assertEquals("Unexpected content", 0x00BB00AA00FF00EEL, buffer.getLong());
    }

    @Test
    public void testReadSimpleLong() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.writeLong(0x00BB00AA00FF00EEL);
            }
            rawFile.flush();
        }

        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, false)) {
                Assert.assertEquals("Unexpected content", 0x00BB00AA00FF00EEL, access.readLong());
            }
        }
    }

    @Test
    public void testWriteSimpleFloat() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.writeFloat(5.5f);
            }
            rawFile.flush();
        }
        Assert.assertTrue("FileBackend has not been created", file.exists());
        Assert.assertEquals("Unexpected file length", (long) RawFileBlock.BLOCK_SIZE, file.length());
        byte[] content = Files.readAllBytes(file.toPath());
        Assert.assertEquals("Unexpected content length", RawFileBlock.BLOCK_SIZE, content.length);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        Assert.assertTrue("Unexpected content", 5.5f == buffer.getFloat());
    }

    @Test
    public void testReadSimpleFloat() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.writeFloat(5.5f);
            }
            rawFile.flush();
        }

        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, false)) {
                Assert.assertTrue("Unexpected content", 5.5f == access.readFloat());
            }
        }
    }

    @Test
    public void testWriteSimpleDouble() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.writeDouble(5.5d);
            }
            rawFile.flush();
        }
        Assert.assertTrue("FileBackend has not been created", file.exists());
        Assert.assertEquals("Unexpected file length", (long) RawFileBlock.BLOCK_SIZE, file.length());
        byte[] content = Files.readAllBytes(file.toPath());
        Assert.assertEquals("Unexpected content length", RawFileBlock.BLOCK_SIZE, content.length);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        Assert.assertTrue("Unexpected content", 5.5f == buffer.getDouble());
    }

    @Test
    public void testReadSimpleDouble() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.writeDouble(5.5d);
            }
            rawFile.flush();
        }

        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, false)) {
                Assert.assertTrue("Unexpected content", 5.5d == access.readDouble());
            }
        }
    }

    @Test
    public void testWriteWithinBlock() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.writeInt(55);
                access.skip(4);
                access.writeInt(66);
            }
            rawFile.flush();
        }

        Assert.assertTrue("FileBackend has not been created", file.exists());
        Assert.assertEquals("Unexpected file length", (long) RawFileBlock.BLOCK_SIZE, file.length());
        byte[] content = Files.readAllBytes(file.toPath());
        Assert.assertEquals("Unexpected content length", RawFileBlock.BLOCK_SIZE, content.length);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        Assert.assertEquals("Unexpected content", 55, buffer.getInt());
        Assert.assertEquals("Unexpected content", 0, buffer.getInt());
        Assert.assertEquals("Unexpected content", 66, buffer.getInt());
    }

    @Test
    public void testWriteTwoBlock() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.writeInt(55);
            }
            try (IOAccess access = rawFile.access(RawFileBlock.BLOCK_SIZE, 12, true)) {
                access.writeInt(66);
            }
            rawFile.flush();
        }
        Assert.assertTrue("FileBackend has not been created", file.exists());
        Assert.assertEquals("Unexpected file length", (long) RawFileBlock.BLOCK_SIZE * 2, file.length());
        byte[] content = Files.readAllBytes(file.toPath());
        Assert.assertEquals("Unexpected content length", RawFileBlock.BLOCK_SIZE * 2, content.length);
        ByteBuffer buffer = ByteBuffer.wrap(content);
        Assert.assertEquals("Unexpected content", 55, buffer.getInt());
        buffer.position(RawFileBlock.BLOCK_SIZE);
        Assert.assertEquals("Unexpected content", 66, buffer.getInt());
    }

    @Test
    public void testReadWrittenData() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.writeInt(55);
            }
            try (IOAccess access = rawFile.access(RawFileBlock.BLOCK_SIZE, 12, true)) {
                access.writeInt(66);
            }
            try (IOAccess access = rawFile.access(0, 12, false)) {
                Assert.assertEquals("Unexpected content", 55, access.readInt());
            }
            try (IOAccess access = rawFile.access(RawFileBlock.BLOCK_SIZE, 12, false)) {
                Assert.assertEquals("Unexpected content", 66, access.readInt());
            }
        }
    }

    @Test
    public void testReload() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, true)) {
                access.writeInt(55);
            }
            try (IOAccess access = rawFile.access(RawFileBlock.BLOCK_SIZE, 12, true)) {
                access.writeInt(66);
            }
            rawFile.flush();
        }

        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            try (IOAccess access = rawFile.access(0, 12, false)) {
                Assert.assertEquals("Unexpected content", 55, access.readInt());
            }
            try (IOAccess access = rawFile.access(RawFileBlock.BLOCK_SIZE, 12, false)) {
                Assert.assertEquals("Unexpected content", 66, access.readInt());
            }
        }
    }

    @Test
    public void testConcurrentWriteRead() throws IOException {
        File file = File.createTempFile("test", ".bin");
        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            IOAccess access1 = rawFile.access(0, 4, true);
            IOAccess access2 = rawFile.access(4, 4, true);
            access1.writeInt(4);
            access2.writeInt(5);
            access1.close();
            access2.close();
            rawFile.flush();
        }

        try (RawFileThreadSafe rawFile = new RawFileThreadSafe(new RawFileBuffered(file, true))) {
            IOAccess access1 = rawFile.access(0, 4, false);
            IOAccess access2 = rawFile.access(4, 4, false);
            Assert.assertEquals("Unexpected content", 4, access1.readInt());
            Assert.assertEquals("Unexpected content", 5, access2.readInt());
            access1.close();
            access2.close();
        }
    }
}
