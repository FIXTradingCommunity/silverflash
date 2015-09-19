/**
 *    Copyright 2015 FIX Protocol Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.fixtrading.silverflash.fixp.store;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.fixtrading.silverflash.fixp.store.CassandraMessageStore;
import org.fixtrading.silverflash.fixp.store.MessageStoreResult;
import org.fixtrading.silverflash.fixp.store.StoreException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CassandraMessageStoreTest {

  private static String contactPoints = "localhost";

  private final Consumer<MessageStoreResult> consumer = new Consumer<MessageStoreResult>() {

    public void accept(MessageStoreResult result) {
      List<ByteBuffer> list = result.getMessageList(fromSeqNo + 20, 10);
      for (ByteBuffer buffer : list) {
        found++;
      }
      result.finishedRetrieving();
    }
  };

  private int found = 0;
  private int countRequested;
  private long fromSeqNo;
  private CassandraMessageStore store;
  private static Process process = null;

  @BeforeClass
  public static void setUpOnce() throws Exception {
    final Runtime runtime = Runtime.getRuntime();
    // Assumes that Cassandra executable in on the path
    final String command = "cassandra";
    process = runtime.exec(command);
    System.out.println(process.isAlive() ? "Cassandra is alive" : "Cassandra not started");
    Thread.sleep(30 * 1000);
  }

  @AfterClass
  public static void tearDownOnce() throws InterruptedException {
    if (process != null) {
      process.destroy();
      process.waitFor(10 * 1000, TimeUnit.MILLISECONDS);
    }
  }

  @Before
  public void setUp() throws Exception {
    store = new CassandraMessageStore(contactPoints);
    store.open().get();
  }

  @After
  public void tearDown() throws Exception {
    store.close();
  }

  @Test
  public void testInsertAndRetrieve() throws InterruptedException, StoreException {
    UUID sessionId = UUID.randomUUID();
    ByteBuffer message = ByteBuffer.allocate(1024);
    message.put("This is a test message to save and retrieve".getBytes());
    message.flip();

    final int maxSeqNo = 1000;
    for (long seqNo = 1; seqNo <= maxSeqNo; seqNo++) {
      store.insertMessage(sessionId, seqNo, message);
    }

    long seqNo = store.retrieveMaxSeqNo(sessionId);
    assertEquals(maxSeqNo, seqNo);

    MessageStoreResult request = new MessageStoreResult(sessionId);
    fromSeqNo = 501L;
    countRequested = 35;
    assertTrue(request.setRequest(System.currentTimeMillis(), fromSeqNo, countRequested));

    store.retrieveMessagesAsync(request, consumer);
    Thread.sleep(5000L);
    assertEquals(10, found);

    // Reuse same request object for another query
    found = 0;
    fromSeqNo = 777L;
    countRequested = 50;
    assertTrue(request.setRequest(System.currentTimeMillis(), fromSeqNo, countRequested));
    store.retrieveMessagesAsync(request, consumer);
    Thread.sleep(5000L);
    assertEquals(10, found);
  }

}
