package org.fixtrading.silverflash.fixp.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import org.fixtrading.silverflash.fixp.store.InMemoryMessageStore;
import org.fixtrading.silverflash.fixp.store.MessageStoreResult;
import org.fixtrading.silverflash.fixp.store.StoreException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
