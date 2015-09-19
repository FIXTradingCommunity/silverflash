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

package org.fixtrading.silverflash.fixp;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.SERVICE_STORE_RETREIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.fixtrading.silverflash.Service;
import org.fixtrading.silverflash.fixp.FixpSession;
import org.fixtrading.silverflash.fixp.Retransmitter;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.fixp.Sessions;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageType;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.RetransmissionRequestEncoder;
import org.fixtrading.silverflash.fixp.store.InMemoryMessageStore;
import org.fixtrading.silverflash.fixp.store.MessageStore;
import org.fixtrading.silverflash.fixp.store.StoreException;
import org.fixtrading.silverflash.reactor.ByteBufferDispatcher;
import org.fixtrading.silverflash.reactor.ByteBufferPayload;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Topic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * @author Don Mendelson
 *
 */
public class RetransmitterTest {

  private Retransmitter retransmitter;
  private EventReactor<ByteBuffer> reactor;
  private MessageStore store;
  private FixpSession session;
  private Sessions sessions;
  private UUID uuid = UUID.randomUUID();
  private long lastRequestTimestamp;

  @Before
  public void setUp() throws Exception {
    reactor =
        EventReactor.builder().withDispatcher(new ByteBufferDispatcher())
            .withPayloadAllocator(new ByteBufferPayload(2048)).build();

    reactor.setTrace(true);
    CompletableFuture<? extends EventReactor<ByteBuffer>> future1 = reactor.open();

    store = new InMemoryMessageStore();
    CompletableFuture<? extends Service> future2 = store.open();

    session = mock(FixpSession.class);
    when(session.getSessionId()).thenReturn(uuid);
    sessions = new Sessions();
    sessions.addSession(session);
    retransmitter = new Retransmitter(reactor, store, sessions);
    CompletableFuture<Retransmitter> future3 = retransmitter.open();

    CompletableFuture.allOf(future1, future2, future3).get();

    lastRequestTimestamp = System.nanoTime();
  }

  @After
  public void tearDown() throws Exception {
    reactor.close();
    retransmitter.close();
    store.close();
  }

  @Test
  public void testRetransmit() throws StoreException, InterruptedException, IOException {
    assertEquals(session, sessions.getSession(uuid));

    ByteBuffer message = ByteBuffer.allocate(1024);
    message.put("The quick brown fox".getBytes());
    for (long seqNo = 1; seqNo < 1001; seqNo++) {
      store.insertMessage(uuid, seqNo, message);
    }

    notifyGap(350, 20);
    Thread.sleep(2000);

    ArgumentCaptor<ByteBuffer[]> messages = ArgumentCaptor.forClass(ByteBuffer[].class);
    ArgumentCaptor<Long> timestamp = ArgumentCaptor.forClass(Long.class);
    verify(session).resend(messages.capture(), anyInt(), anyInt(), anyLong(), timestamp.capture());
    assertTrue(messages.getValue().length > 0);
    assertEquals(lastRequestTimestamp, timestamp.getValue().longValue());
  }

  private void notifyGap(long fromSeqNo, int count) {
    ByteBuffer retransmissionRequestBuffer = ByteBuffer.allocate(46).order(ByteOrder.nativeOrder());
    MessageEncoder messageEncoder = new MessageEncoder();
    RetransmissionRequestEncoder retransmissionRequestEncoder =
        (RetransmissionRequestEncoder) messageEncoder.attachForEncode(retransmissionRequestBuffer,
            0, MessageType.RETRANSMIT_REQUEST);

    retransmissionRequestEncoder.setSessionId(SessionId.UUIDAsBytes(uuid));
    retransmissionRequestEncoder.setTimestamp(lastRequestTimestamp);
    retransmissionRequestEncoder.setFromSeqNo(fromSeqNo);
    retransmissionRequestEncoder.setCount(count);

    Topic retrieveTopic = SessionEventTopics.getTopic(SERVICE_STORE_RETREIVE);
    reactor.post(retrieveTopic, retransmissionRequestBuffer);
  }
}
