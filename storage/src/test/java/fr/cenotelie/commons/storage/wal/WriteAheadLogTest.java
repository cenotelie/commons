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
import fr.cenotelie.commons.storage.memory.InMemoryStore;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Basic test suite for the Write-ahead log
 *
 * @author Laurent Wouters
 */
public class WriteAheadLogTest {

    @Test
    public void testLinearTransactions() throws IOException {
        InMemoryStore base = new InMemoryStore();
        InMemoryStore log = new InMemoryStore();
        WriteAheadLog wal = new WriteAheadLog(base, log);

        try (Transaction transaction = wal.newTransaction(true)) {
            try (StorageAccess access = transaction.access(0, 4, true)) {
                access.writeInt(0xFFFFFFFF);
            }
            transaction.commit();
        }
        try (Transaction transaction = wal.newTransaction(false)) {
            try (StorageAccess access = transaction.access(0, 4, false)) {
                int value = access.readInt();
                Assert.assertEquals(0xFFFFFFFF, value);
            }
            transaction.commit();
        }

        wal.close();
    }

    @Test
    public void testConcurrentTransactions() throws IOException {
        InMemoryStore base = new InMemoryStore();
        InMemoryStore log = new InMemoryStore();
        WriteAheadLog wal = new WriteAheadLog(base, log);

        Transaction transaction1 = wal.newTransaction(true);
        Transaction transaction2 = wal.newTransaction(false);

        try (StorageAccess access = transaction1.access(0, 4, true)) {
            access.writeInt(0xFFFFFFFF);
        }
        transaction1.commit();
        transaction1.close();

        try (StorageAccess access = transaction2.access(0, 4, false)) {
            int value = access.readInt();
            Assert.assertEquals(0, value);
        }
        transaction2.commit();
        transaction2.close();

        wal.close();
    }
}
