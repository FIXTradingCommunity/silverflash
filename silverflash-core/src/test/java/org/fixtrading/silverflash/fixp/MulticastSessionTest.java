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

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.UUID;

import org.fixtrading.silverflash.MessageConsumer;
import org.fixtrading.silverflash.Session;
import org.fixtrading.silverflash.buffer.SingleBufferSupplier;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderWithFrame;
import org.fixtrading.silverflash.reactor.ByteBufferDispatcher;
import org.fixtrading.silverflash.reactor.ByteBufferPayload;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.transport.PipeTransport;
import org.fixtrading.silverflash.transport.Transport;
import org.fixtrading.silverflash.transport.TransportDecorator;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class MulticastSessionTest {

  class TestReceiver implements MessageConsumer<UUID> {
    int bytesReceived = 0;
    private byte[] dst = new byte[16 * 1024];

    public int getBytesReceived() {
      return bytesReceived;
    }

    @Override
    public void accept(ByteBuffer buf, Session<UUID> session, long seqNo) {
      int bytesToReceive = buf.remaining();
      bytesReceived += bytesToReceive;
      buf.get(dst, 0, bytesToReceive);
    }
  }

  static final byte STREAM_ID = 99;

  private static final int templateId = 22;
  private static final int schemaVersion = 0;
  private static final int schemaId = 33;

  private Engine engine;
  private EventReactor<ByteBuffer> reactor2;
  private PipeTransport memoryTransport;
  private int messageCount = Byte.MAX_VALUE;
  private byte[][] messages;
  private int keepAliveInterval = 500;

  @Before
  public void setUp() throws Exception {
    engine =
        Engine.builder().build();
    engine.open();

    reactor2 =
        EventReactor.builder().withDispatcher(new ByteBufferDispatcher())
            .withPayloadAllocator(new ByteBufferPayload(2048)).build();

    reactor2.open().get();

    memoryTransport = new PipeTransport(engine.getIOReactor().getSelector());

    messages = new byte[messageCount][];
    for (int i = 0; i < messageCount; ++i) {
      messages[i] = new byte[i];
      Arrays.fill(messages[i], (byte) i);
    }
  }

  @After
  public void tearDown() throws Exception {
    engine.close();
    reactor2.close();
  }

  @Ignore
  @Test
  public void multicast() throws Exception {
    Transport serverTransport = memoryTransport.getServerTransport();
    TransportDecorator nonFifoServerTransport = new TransportDecorator(serverTransport, false);
    TestReceiver serverReceiver = new TestReceiver();
    String topic = "options";

    FixpSession producerSession =
        FixpSession
            .builder()
            .withReactor(engine.getReactor())
            .withTransport(nonFifoServerTransport)
            .withBufferSupplier(
                new SingleBufferSupplier(ByteBuffer.allocate(16 * 1024).order(
                    ByteOrder.nativeOrder())))
            .withOutboundFlow(FlowType.IDEMPOTENT)
            .withOutboundKeepaliveInterval(3000)
            .asMulticastPublisher()
            .withTopic(topic)
            .withSessionId(SessionId.generateUUID())
            .build();

    Transport clientTransport = memoryTransport.getClientTransport();
    TransportDecorator nonFifoClientTransport = new TransportDecorator(clientTransport, false);
    TestReceiver clientReceiver = new TestReceiver();

    FixpSession consumerSession =
        FixpSession
            .builder()
            .withReactor(reactor2)
            .withTransport(nonFifoClientTransport)
            .withBufferSupplier(
                new SingleBufferSupplier(ByteBuffer.allocate(16 * 1024).order(
                    ByteOrder.nativeOrder()))).withMessageConsumer(clientReceiver)
            .asMulticastConsumer()
            .withTopic(topic)
            .build();

    consumerSession.open();
    
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
    }
    producerSession.open();

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
    }
 
    ByteBuffer buf = ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder());
    int bytesSent = 0;
    for (int i = 0; i < messageCount; ++i) {
      buf.clear();
      encodeApplicationMessageWithFrame(buf, messages[i]);
      bytesSent += buf.position();
      producerSession.send(buf);
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {

    }
    assertEquals(bytesSent, serverReceiver.getBytesReceived());

    consumerSession.close();
    producerSession.close();
   }

  private void encodeApplicationMessageWithFrame(ByteBuffer buf, byte[] message) {
    MessageHeaderWithFrame.encode(buf, buf.position(), message.length, templateId, schemaId,
        schemaVersion, message.length + MessageHeaderWithFrame.getLength());
    buf.put(message);
  }

}
