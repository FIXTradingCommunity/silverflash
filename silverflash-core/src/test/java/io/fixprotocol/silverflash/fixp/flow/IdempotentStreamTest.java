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

package io.fixprotocol.silverflash.fixp.flow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.fixprotocol.silverflash.MessageConsumer;
import io.fixprotocol.silverflash.Session;
import io.fixprotocol.silverflash.buffer.SingleBufferSupplier;
import io.fixprotocol.silverflash.fixp.Engine;
import io.fixprotocol.silverflash.fixp.SessionId;
import io.fixprotocol.silverflash.fixp.flow.FlowBuilder;
import io.fixprotocol.silverflash.fixp.flow.FlowReceiverBuilder;
import io.fixprotocol.silverflash.fixp.flow.IdempotentFlowReceiver;
import io.fixprotocol.silverflash.fixp.flow.IdempotentFlowSender;
import io.fixprotocol.silverflash.fixp.flow.SimplexSequencer;
import io.fixprotocol.silverflash.fixp.flow.IdempotentFlowSender.Builder;
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderDecoder;
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderEncoder;
import io.fixprotocol.silverflash.frame.MessageLengthFrameEncoder;
import io.fixprotocol.silverflash.frame.MessageLengthFrameSpliterator;
import io.fixprotocol.silverflash.transport.PipeTransport;
import io.fixprotocol.silverflash.transport.Transport;
import io.fixprotocol.silverflash.transport.TransportConsumer;

@Ignore
public class IdempotentStreamTest {

  class TestReceiver implements MessageConsumer<UUID> {
    int bytesReceived = 0;
    private final DirectBuffer immutableBuffer = new UnsafeBuffer(new byte[0]);
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    int messagesReceived = 0;

    @Override
    public void accept(ByteBuffer buffer, Session<UUID> session, long seqNo) {
      immutableBuffer.wrap(buffer);
      messageHeaderDecoder.wrap(immutableBuffer, buffer.position());
      if (messageHeaderDecoder.schemaId() == schemaId) {

        bytesReceived += messageHeaderDecoder.blockLength();
       } else {
        assertEquals("Schema mismatch", schemaId, messageHeaderDecoder.schemaId());
      }
      messagesReceived++;
    }

    public int getBytesReceived() {
      return bytesReceived;
    }

    public int getMessagesReceived() {
      return messagesReceived;
    }
  }

  class TestSession implements Session<UUID> {

    private final UUID sessionId = UUID.randomUUID();

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.AutoCloseable#close()
     */
    public void close() throws Exception {
      // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see io.fixprotocol.silverflash.Session#getSessionId()
     */
    public UUID getSessionId() {
      return sessionId;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.fixprotocol.silverflash.Session#open()
     */
    public CompletableFuture<? extends Session<UUID>> open() {
      return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see io.fixprotocol.silverflash.Sender#send(java.nio.ByteBuffer)
     */
    public long send(ByteBuffer message) throws IOException {
      // TODO Auto-generated method stub
      return 0;
    }

  }

  private Engine engine;
  private MessageLengthFrameEncoder frameEncoder = new MessageLengthFrameEncoder();
  private int keepAliveInterval = 500;
  private PipeTransport memoryTransport;
  final int messageCount = 200;
  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private byte[][] messages;
  private MutableDirectBuffer mutableBuffer = new UnsafeBuffer(new byte[0]);
  private final int schemaId = 13;
  private final int schemaVersion = 1;
  private TestReceiver serverReceiver;
  private Transport serverTransport;
  private IdempotentFlowReceiver streamReceiver;
  private final int templateId = 26;

  @Test
  public void duplicateSend() throws Exception {

    TransportConsumer frameReceiver = new TransportConsumer() {
      private final MessageLengthFrameSpliterator spliter = new MessageLengthFrameSpliterator();

      @Override
      public void accept(ByteBuffer buffer) {
        spliter.wrap(buffer);
        spliter.forEachRemaining(streamReceiver);
      }

      @Override
      public void connected() {
        // TODO Auto-generated method stub

      }

      @Override
      public void disconnected() {
        // TODO Auto-generated method stub

      }
    };

    serverTransport.open(
        new SingleBufferSupplier(ByteBuffer.allocate(16 * 1024).order(ByteOrder.nativeOrder())),
        frameReceiver);

    Transport clientTransport = memoryTransport.getClientTransport();
    TransportConsumer callbackReceiver = new TransportConsumer() {

      @Override
      public void accept(ByteBuffer t) {
        fail("Callback not expected");
      }

      @Override
      public void connected() {
        // TODO Auto-generated method stub

      }

      @Override
      public void disconnected() {
        // TODO Auto-generated method stub

      }

    };
    clientTransport.open(
        new SingleBufferSupplier(ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder())),
        callbackReceiver).get();
    UUID uuid = SessionId.generateUUID();
    final SimplexSequencer sequencer = new SimplexSequencer(frameEncoder);

    Builder<IdempotentFlowSender, FlowBuilder> builder = IdempotentFlowSender.builder();
    builder.withKeepaliveInterval(keepAliveInterval).withReactor(engine.getReactor())
    .withSessionId(uuid).withTransport(clientTransport).withSequencer(sequencer);
    IdempotentFlowSender sender = builder.build();

    ByteBuffer buf = ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder());
    buf.clear();
    byte[] msg = "Hello world".getBytes();
    encodeApplicationMessage(buf, msg);
    sender.send(buf);
    buf.position(buf.limit());
    // Resend with nextSeqNo=1
    sequencer.setNextSeqNo(1);
    sender.send(buf);

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {

    }

    assertEquals(1, serverReceiver.getMessagesReceived());
  }

  void encodeApplicationMessage(ByteBuffer buffer, byte[] message) {
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
  }

  @Test
  public void implicitSequenceSend() throws Exception {

    TransportConsumer frameReceiver = new TransportConsumer() {
      private final MessageLengthFrameSpliterator spliter = new MessageLengthFrameSpliterator();

      @Override
      public void accept(ByteBuffer buffer) {
        spliter.wrap(buffer);
        spliter.forEachRemaining(streamReceiver);
      }

      @Override
      public void connected() {
        // TODO Auto-generated method stub

      }

      @Override
      public void disconnected() {
        // TODO Auto-generated method stub

      }
    };

    serverTransport.open(
        new SingleBufferSupplier(ByteBuffer.allocate(16 * 1024).order(ByteOrder.nativeOrder())),
        frameReceiver);

    Transport clientTransport = memoryTransport.getClientTransport();
    TransportConsumer callbackReceiver = new TransportConsumer() {

      @Override
      public void accept(ByteBuffer t) {
        fail("Callback not expected");
      }

      @Override
      public void connected() {
        // TODO Auto-generated method stub

      }

      @Override
      public void disconnected() {
        // TODO Auto-generated method stub

      }

    };
    clientTransport.open(
        new SingleBufferSupplier(ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder())),
        callbackReceiver);
    UUID uuid = SessionId.generateUUID();
    SimplexSequencer sequencer = new SimplexSequencer(frameEncoder);

    Builder<IdempotentFlowSender, FlowBuilder> builder = IdempotentFlowSender.builder();
    builder.withKeepaliveInterval(keepAliveInterval).withReactor(engine.getReactor())
    .withSessionId(uuid).withTransport(clientTransport).withSequencer(sequencer);
    IdempotentFlowSender sender = builder.build();

    ByteBuffer buf = ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder());
    int bytesSent = 0;
    for (int i = 0; i < messageCount; ++i) {
      buf.clear();
      encodeApplicationMessage(buf, messages[i]);
      sender.send(buf);
      bytesSent += messages[i].length;
    }

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {

    }

    assertEquals(messageCount, serverReceiver.getMessagesReceived());
    assertEquals(bytesSent, serverReceiver.getBytesReceived());
  }

  @Before
  public void setUp() throws Exception {
    messages = new byte[messageCount][];
    for (int i = 0; i < messageCount; ++i) {
      messages[i] = new byte[i];
      Arrays.fill(messages[i], (byte) i);
    }

    engine = Engine.builder().build();
    engine.open();

    memoryTransport = new PipeTransport(engine.getIOReactor().getSelector());
    serverTransport = memoryTransport.getServerTransport();
    serverReceiver = new TestReceiver();
    
    IdempotentFlowReceiver.Builder<IdempotentFlowReceiver, ? extends FlowReceiverBuilder> builder = IdempotentFlowReceiver.builder();
    builder.withMessageConsumer(serverReceiver)
    .withSession(new TestSession())
    .withKeepaliveInterval(keepAliveInterval)
    .withReactor(engine.getReactor())
    .withTransport(serverTransport)
    .withMessageFrameEncoder(frameEncoder);
     streamReceiver = builder.build();
  }

  @After
  public void tearDown() {
    engine.close();
  }
}
