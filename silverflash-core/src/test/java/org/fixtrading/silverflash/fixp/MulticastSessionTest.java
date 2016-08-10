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

package org.fixtrading.silverflash.fixp;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.fixtrading.silverflash.MessageConsumer;
import org.fixtrading.silverflash.Session;
import org.fixtrading.silverflash.buffer.SingleBufferSupplier;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderEncoder;
import org.fixtrading.silverflash.frame.MessageLengthFrameEncoder;
import org.fixtrading.silverflash.transport.PipeTransport;
import org.fixtrading.silverflash.transport.Transport;
import org.fixtrading.silverflash.transport.TransportDecorator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MulticastSessionTest {

  class TestReceiver implements MessageConsumer<UUID> {
    int bytesReceived = 0;
    private byte[] dst = new byte[16 * 1024];
    int msgsReceived;

    public int getBytesReceived() {
      return bytesReceived;
    }

    @Override
    public void accept(ByteBuffer buf, Session<UUID> session, long seqNo) {
      int bytesToReceive = buf.remaining();
      bytesReceived += bytesToReceive;
      buf.get(dst, 0, bytesToReceive);
      msgsReceived++;
    }

    public int getMsgsReceived() {
      return msgsReceived;
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
  private MessageLengthFrameEncoder frameEncoder = new MessageLengthFrameEncoder();
  private MutableDirectBuffer mutableBuffer = new UnsafeBuffer(new byte[0]);
  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private int keepAliveInterval = 500;

  @Before
  public void setUp() throws Exception {
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
            .withOutboundFlow(FlowType.Idempotent)
            .withOutboundKeepaliveInterval(3000)
            .asMulticastPublisher()
            .withTopic(topic)
            .withMessageFrameEncoder(new MessageLengthFrameEncoder())
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
      Thread.sleep(1000);
    } catch (InterruptedException e) {

    }
    assertEquals(messageCount, clientReceiver.getMsgsReceived());


    consumerSession.close();
    producerSession.close();
   }

  private long encodeApplicationMessageWithFrame(ByteBuffer buffer, byte[] message) {
    int offset = 0;
    mutableBuffer.wrap(buffer);
    frameEncoder.wrap(buffer, offset).encodeFrameHeader();
    offset += frameEncoder.getHeaderLength();
    messageHeaderEncoder.wrap(mutableBuffer, offset);
    messageHeaderEncoder.blockLength(message.length)
        .templateId(templateId).schemaId(schemaId)
        .version(schemaVersion);
    offset += MessageHeaderEncoder.ENCODED_LENGTH; 
    buffer.position(offset);
    buffer.put(message, 0, message.length);
    frameEncoder.setMessageLength(message.length + MessageHeaderEncoder.ENCODED_LENGTH);
    frameEncoder.encodeFrameTrailer();
    return frameEncoder.getEncodedLength();
  }


}
