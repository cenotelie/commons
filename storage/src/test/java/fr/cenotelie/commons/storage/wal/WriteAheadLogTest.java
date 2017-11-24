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

import fr.cenotelie.commons.storage.Access;
import fr.cenotelie.commons.storage.Constants;
import fr.cenotelie.commons.storage.Transaction;
import fr.cenotelie.commons.storage.files.RawFileBuffered;
import fr.cenotelie.commons.storage.memory.InMemoryStore;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ConcurrentModificationException;

/**
 * Basic test suite for the Write-ahead log
 *
 * @author Laurent Wouters
 */
public class WriteAheadLogTest {
    @Test
    public void testAtomicityCommit() throws IOException {
        InMemoryStore base = new InMemoryStore();
        InMemoryStore log = new InMemoryStore();
        WriteAheadLog wal = new WriteAheadLog(base, log, false);

        Transaction transaction = wal.newTransaction(true);
        try (Access access = transaction.access(0, 4, true)) {
            access.writeInt(0xFFFFFFFF);
        }
        try (Access access = transaction.access(Constants.PAGE_SIZE, 4, true)) {
            access.writeInt(0xFFFFFFFF);
        }

        // check that nothing has been committed yet
        try (Access access = base.access(0, 4, false)) {
            Assert.assertEquals(0, access.readInt());
        }
        try (Access access = base.access(Constants.PAGE_SIZE, 4, false)) {
            Assert.assertEquals(0, access.readInt());
        }

        // commit
        transaction.commit();
        transaction.close();

        // check that we can read the changes through a transaction
        transaction = wal.newTransaction(false);
        try (Access access = transaction.access(0, 4, false)) {
            Assert.assertEquals(0xFFFFFFFF, access.readInt());
        }
        try (Access access = transaction.access(Constants.PAGE_SIZE, 4, false)) {
            Assert.assertEquals(0xFFFFFFFF, access.readInt());
        }
        transaction.close();

        // run a checkpoint
        wal.cleanup(true);

        // check that we now can read the values
        try (Access access = base.access(0, 4, false)) {
            Assert.assertEquals(0xFFFFFFFF, access.readInt());
        }
        try (Access access = base.access(Constants.PAGE_SIZE, 4, false)) {
            Assert.assertEquals(0xFFFFFFFF, access.readInt());
        }

        wal.close();
    }

    @Test
    public void testAtomicityAbort() throws IOException {
        InMemoryStore base = new InMemoryStore();
        InMemoryStore log = new InMemoryStore();
        WriteAheadLog wal = new WriteAheadLog(base, log, false);

        Transaction transaction = wal.newTransaction(true);
        try (Access access = transaction.access(0, 4, true)) {
            access.writeInt(0xFFFFFFFF);
        }
        // check that nothing has been committed yet
        try (Access access = base.access(0, 4, false)) {
            Assert.assertEquals(0, access.readInt());
        }
        // abort the transaction
        transaction.abort();
        transaction.close();

        // check that no change is visible through a transaction
        transaction = wal.newTransaction(false);
        try (Access access = transaction.access(0, 4, false)) {
            Assert.assertEquals(0, access.readInt());
        }
        transaction.close();

        // run a checkpoint
        wal.cleanup(true);

        // check that the storage is intact
        try (Access access = base.access(0, 4, false)) {
            Assert.assertEquals(0, access.readInt());
        }

        wal.close();
    }

    @Test
    public void testIsolationLinearTransactions() throws IOException {
        InMemoryStore base = new InMemoryStore();
        InMemoryStore log = new InMemoryStore();
        WriteAheadLog wal = new WriteAheadLog(base, log, false);

        try (Transaction transaction = wal.newTransaction(true)) {
            try (Access access = transaction.access(0, 4, true)) {
                access.writeInt(0xFFFFFFFF);
            }
            transaction.commit();
        }
        try (Transaction transaction = wal.newTransaction(false)) {
            try (Access access = transaction.access(0, 4, false)) {
                int value = access.readInt();
                Assert.assertEquals(0xFFFFFFFF, value);
            }
            transaction.commit();
        }

        wal.close();
    }

    @Test
    public void testIsolationConcurrentTransactions() throws IOException {
        InMemoryStore base = new InMemoryStore();
        InMemoryStore log = new InMemoryStore();
        WriteAheadLog wal = new WriteAheadLog(base, log, false);

        Transaction transaction1 = wal.newTransaction(true);
        Transaction transaction2 = wal.newTransaction(false);

        try (Access access = transaction1.access(0, 4, true)) {
            access.writeInt(0xFFFFFFFF);
        }
        transaction1.commit();
        transaction1.close();

        try (Access access = transaction2.access(0, 4, false)) {
            int value = access.readInt();
            Assert.assertEquals(0, value);
        }
        transaction2.commit();
        transaction2.close();

        wal.close();
    }

    @Test
    public void testIsolationConcurrentWriting() throws IOException {
        InMemoryStore base = new InMemoryStore();
        InMemoryStore log = new InMemoryStore();
        WriteAheadLog wal = new WriteAheadLog(base, log, false);

        Transaction transaction1 = wal.newTransaction(true);
        Transaction transaction2 = wal.newTransaction(true);

        try (Access access = transaction1.access(0, 4, true)) {
            access.writeInt(0xFFFFFFFF);
        }
        transaction1.commit();
        transaction1.close();

        try (Access access = transaction2.access(0, 4, true)) {
            access.writeInt(0xFFFFFFFE);
        }
        boolean catched = false;
        try {
            transaction2.commit();
        } catch (ConcurrentModificationException exception) {
            catched = true;
        }
        transaction2.close();
        Assert.assertTrue(catched);

        wal.close();
    }

    @Test
    public void testDurabilityNormalClose() throws IOException {
        RawFileBuffered base = new RawFileBuffered(File.createTempFile("test", ".bin"), true);
        RawFileBuffered log = new RawFileBuffered(File.createTempFile("test", ".bin"), true);
        WriteAheadLog wal = new WriteAheadLog(base, log, false);

        // write the data
        try (Transaction transaction = wal.newTransaction(true)) {
            try (Access access = transaction.access(0, 4, true)) {
                access.writeInt(0xFFFFFFFF);
            }
            try (Access access = transaction.access(Constants.PAGE_SIZE, 4, true)) {
                access.writeInt(0xFFFFFFFF);
            }
            transaction.commit();
        }
        try (Transaction transaction = wal.newTransaction(true)) {
            try (Access access = transaction.access(4, 4, true)) {
                access.writeInt(0xEEEEEEEE);
            }
            try (Access access = transaction.access(Constants.PAGE_SIZE + 4, 4, true)) {
                access.writeInt(0xEEEEEEEE);
            }
            transaction.commit();
        }

        wal.close(); // also closes the files

        // find the values in the base file
        base = new RawFileBuffered(base.getSystemFile(), false);
        Assert.assertEquals(Constants.PAGE_SIZE + 4 + 4, base.getSize());
        try (Access access = base.access(0, 4, true)) {
            Assert.assertEquals(0xFFFFFFFF, access.readInt());
        }
        try (Access access = base.access(Constants.PAGE_SIZE, 4, true)) {
            Assert.assertEquals(0xFFFFFFFF, access.readInt());
        }
        try (Access access = base.access(4, 4, true)) {
            Assert.assertEquals(0xEEEEEEEE, access.readInt());
        }
        try (Access access = base.access(Constants.PAGE_SIZE + 4, 4, true)) {
            Assert.assertEquals(0xEEEEEEEE, access.readInt());
        }
        base.close();

        // inspect the log file
        log = new RawFileBuffered(log.getSystemFile(), false);
        Assert.assertEquals(32, log.getSize());
        try (Access access = log.access(0, 32, true)) {
            access.skip(16);
            Assert.assertEquals(0L, access.readLong());
            Assert.assertEquals(0L, access.readLong());
        }
        log.close();
    }

    @Test
    public void testDurabilityCrashNoCheckpoint() throws IOException {
        RawFileBuffered base = new RawFileBuffered(File.createTempFile("test", ".bin"), true);
        RawFileBuffered log = new RawFileBuffered(File.createTempFile("test", ".bin"), true);
        WriteAheadLog wal = new WriteAheadLog(base, log, false);

        // write the data
        try (Transaction transaction = wal.newTransaction(true)) {
            try (Access access = transaction.access(0, 4, true)) {
                access.writeInt(0xFFFFFFFF);
            }
            try (Access access = transaction.access(Constants.PAGE_SIZE, 4, true)) {
                access.writeInt(0xFFFFFFFF);
            }
            transaction.commit();
        }
        try (Transaction transaction = wal.newTransaction(true)) {
            try (Access access = transaction.access(4, 4, true)) {
                access.writeInt(0xEEEEEEEE);
            }
            try (Access access = transaction.access(Constants.PAGE_SIZE + 4, 4, true)) {
                access.writeInt(0xEEEEEEEE);
            }
            transaction.commit();
        }

        // simulate a crash by closing the files
        base.close();
        log.close();

        // inspect the base file
        base = new RawFileBuffered(base.getSystemFile(), true);
        Assert.assertEquals(0, base.getSize()); // should be empty
        // inspect the log file
        log = new RawFileBuffered(log.getSystemFile(), true);
        Assert.assertTrue(log.getSize() > 8 * 4);
        // reopen the WAL
        wal = new WriteAheadLog(base, log, false);

        // re-inspect the base file
        Assert.assertEquals(Constants.PAGE_SIZE + 4 + 4, base.getSize());
        try (Access access = base.access(0, 4, true)) {
            Assert.assertEquals(0xFFFFFFFF, access.readInt());
        }
        try (Access access = base.access(Constants.PAGE_SIZE, 4, true)) {
            Assert.assertEquals(0xFFFFFFFF, access.readInt());
        }
        try (Access access = base.access(4, 4, true)) {
            Assert.assertEquals(0xEEEEEEEE, access.readInt());
        }
        try (Access access = base.access(Constants.PAGE_SIZE + 4, 4, true)) {
            Assert.assertEquals(0xEEEEEEEE, access.readInt());
        }

        // re-inspect the log
        Assert.assertEquals(32, log.getSize());
        try (Access access = log.access(0, 32, true)) {
            access.skip(16);
            Assert.assertEquals(0L, access.readLong());
            Assert.assertEquals(0L, access.readLong());
        }

        wal.close();
    }

    @Test
    public void testDurabilityCrashAfterCheckpoint() throws IOException {
        RawFileBuffered base = new RawFileBuffered(File.createTempFile("test", ".bin"), true);
        RawFileBuffered log = new RawFileBuffered(File.createTempFile("test", ".bin"), true);
        WriteAheadLog wal = new WriteAheadLog(base, log, false);

        // write the data
        try (Transaction transaction = wal.newTransaction(true)) {
            try (Access access = transaction.access(0, 4, true)) {
                access.writeInt(0xFFFFFFFF);
            }
            try (Access access = transaction.access(Constants.PAGE_SIZE, 4, true)) {
                access.writeInt(0xFFFFFFFF);
            }
            transaction.commit();
        }
        // do the checkpoint
        wal.cleanup(true);
        // write more data
        try (Transaction transaction = wal.newTransaction(true)) {
            try (Access access = transaction.access(4, 4, true)) {
                access.writeInt(0xEEEEEEEE);
            }
            try (Access access = transaction.access(Constants.PAGE_SIZE + 4, 4, true)) {
                access.writeInt(0xEEEEEEEE);
            }
            transaction.commit();
        }

        // simulate a crash by closing the files
        base.close();
        log.close();

        // inspect the base file
        base = new RawFileBuffered(base.getSystemFile(), true);
        Assert.assertEquals(Constants.PAGE_SIZE + 4, base.getSize()); // should be empty
        // inspect the log file
        log = new RawFileBuffered(log.getSystemFile(), true);
        Assert.assertTrue(log.getSize() > 8 * 4);
        // reopen the WAL
        wal = new WriteAheadLog(base, log, false);

        // re-inspect the base file
        Assert.assertEquals(Constants.PAGE_SIZE + 4 + 4, base.getSize());
        try (Access access = base.access(0, 4, true)) {
            Assert.assertEquals(0xFFFFFFFF, access.readInt());
        }
        try (Access access = base.access(Constants.PAGE_SIZE, 4, true)) {
            Assert.assertEquals(0xFFFFFFFF, access.readInt());
        }
        try (Access access = base.access(4, 4, true)) {
            Assert.assertEquals(0xEEEEEEEE, access.readInt());
        }
        try (Access access = base.access(Constants.PAGE_SIZE + 4, 4, true)) {
            Assert.assertEquals(0xEEEEEEEE, access.readInt());
        }

        // re-inspect the log
        Assert.assertEquals(32, log.getSize());
        try (Access access = log.access(0, 32, true)) {
            access.skip(16);
            Assert.assertEquals(0L, access.readLong());
            Assert.assertEquals(0L, access.readLong());
        }

        wal.close();
    }
}
