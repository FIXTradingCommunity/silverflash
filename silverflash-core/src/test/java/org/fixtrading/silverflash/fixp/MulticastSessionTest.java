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
import java.util.concurrent.TimeUnit;

import org.fixtrading.silverflash.MessageConsumer;
import org.fixtrading.silverflash.Session;
import org.fixtrading.silverflash.buffer.SingleBufferSupplier;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.SbeMessageHeaderDecoder;
import org.fixtrading.silverflash.fixp.messages.SbeMessageHeaderEncoder;
import org.fixtrading.silverflash.frame.MessageLengthFrameEncoder;
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

  private Engine engine1, engine2;
  private PipeTransport memoryTransport;
  private int messageCount = Byte.MAX_VALUE;
  private byte[][] messages;
  private MessageLengthFrameEncoder frameEncoder;
  private SbeMessageHeaderEncoder sbeEncoder;

  private int keepAliveInterval = 500;

  @Before
  public void setUp() throws Exception {
    frameEncoder = new MessageLengthFrameEncoder();
    sbeEncoder = new SbeMessageHeaderEncoder();

    engine1 =
        Engine.builder().build();
    engine1.open();

    engine2 =
        Engine.builder().build();
    engine2.open();
    
    memoryTransport = new PipeTransport(engine1.getIOReactor().getSelector());

    messages = new byte[messageCount][];
    for (int i = 0; i < messageCount; ++i) {
      messages[i] = new byte[i];
      Arrays.fill(messages[i], (byte) i);
    }
  }

  @After
  public void tearDown() throws Exception {
    engine1.close();
    engine2.close();
  }

  @Test
  public void multicast() throws Exception {
    Transport serverTransport = memoryTransport.getServerTransport();
    TransportDecorator nonFifoServerTransport = new TransportDecorator(serverTransport, false, false, true);
    TestReceiver serverReceiver = new TestReceiver();
    String topic = "options";

    FixpSession producerSession =
        FixpSession
            .builder()
            .withReactor(engine1.getReactor())
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
    TransportDecorator nonFifoClientTransport = new TransportDecorator(clientTransport, false, true, false);
    TestReceiver clientReceiver = new TestReceiver();

    FixpSession consumerSession =
        FixpSession
            .builder()
            .withReactor(engine2.getReactor())
            .withTransport(nonFifoClientTransport)
            .withBufferSupplier(
                new SingleBufferSupplier(ByteBuffer.allocate(16 * 1024).order(
                    ByteOrder.nativeOrder()))).withMessageConsumer(clientReceiver)
            .asMulticastConsumer()
            .withTopic(topic)
            .build();

    producerSession.open().get(1000, TimeUnit.MILLISECONDS);
    consumerSession.open().get(1000, TimeUnit.MILLISECONDS);   

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      
    }
    
    ByteBuffer buf = ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder());
    int bytesSent = 0;
    for (int i = 0; i < messageCount; ++i) {
      buf.clear();
      bytesSent += encodeApplicationMessageWithFrame(buf, messages[i]);
      producerSession.send(buf);
    }

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {

    }
    assertEquals(bytesSent, clientReceiver.getBytesReceived());

    consumerSession.close();
    producerSession.close();
   }

  private int encodeApplicationMessageWithFrame(ByteBuffer buf, byte[] message) {
    frameEncoder.wrap(buf);
    frameEncoder.encodeFrameHeader();
    sbeEncoder.wrap(buf, frameEncoder.getHeaderLength()).setBlockLength(message.length).setTemplateId(templateId)
        .setSchemaId(schemaId).getSchemaVersion(schemaVersion);
    buf.put(message, 0, message.length);
    final int lengthwithHeader = message.length + SbeMessageHeaderDecoder.getLength();
    frameEncoder.setMessageLength(lengthwithHeader);
    frameEncoder.encodeFrameTrailer();
    return lengthwithHeader;
  }

}
