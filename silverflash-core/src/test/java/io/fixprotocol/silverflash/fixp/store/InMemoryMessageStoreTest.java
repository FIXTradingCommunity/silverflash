/**
 *    Copyright 2015-2016 FIX Protocol Ltd
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

package io.fixprotocol.silverflash.fixp.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.fixprotocol.silverflash.fixp.store.InMemoryMessageStore;
import io.fixprotocol.silverflash.fixp.store.MessageStoreResult;
import io.fixprotocol.silverflash.fixp.store.StoreException;

/**
 * @author Don Mendelson
 *
 */
public class InMemoryMessageStoreTest {

  Consumer<MessageStoreResult> consumer = new Consumer<MessageStoreResult>() {

    public void accept(MessageStoreResult result) {
      List<ByteBuffer> list = result.getMessageList(fromSeqNo + 20, 10);
      for (ByteBuffer buffer : list) {
        found++;
      }
      result.finishedRetrieving();
    }
  };

  int found = 0;
  private int countRequested;
  private long fromSeqNo;
  private InMemoryMessageStore store;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    store = new InMemoryMessageStore();
    store.open().get();
  }

  /**
   * @throws java.lang.Exception
   */
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
    assertEquals(10, found);

    // Reuse same request object for another query
    found = 0;
    fromSeqNo = 777L;
    countRequested = 50;
    assertTrue(request.setRequest(System.currentTimeMillis(), fromSeqNo, countRequested));
    store.retrieveMessagesAsync(request, consumer);
    assertEquals(10, found);
  }

}
