package org.fixtrading.silverflash.fixp.flow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.UUID;

import org.fixtrading.silverflash.MessageConsumer;
import org.fixtrading.silverflash.Session;
import org.fixtrading.silverflash.buffer.SingleBufferSupplier;
import org.fixtrading.silverflash.fixp.Engine;
import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.fixp.flow.IdempotentFlowReceiver;
import org.fixtrading.silverflash.fixp.flow.IdempotentFlowSender;
import org.fixtrading.silverflash.fixp.flow.SimplexSequencer;
import org.fixtrading.silverflash.fixp.frame.FixpWithMessageLengthFrameSpliterator;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderWithFrame;
import org.fixtrading.silverflash.transport.PipeTransport;
import org.fixtrading.silverflash.transport.Transport;
import org.fixtrading.silverflash.transport.TransportConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IdempotentStreamTest {

  class TestSession implements Session<UUID> {

    private final UUID sessionId = UUID.randomUUID();

    /*
     * (non-Javadoc)
     * 
     * @see org.fixtrading.silverflash.Sender#send(java.nio.ByteBuffer)
     */
    public long send(ByteBuffer message) throws IOException {
      // TODO Auto-generated method stub
      return 0;
    }

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
     * @see org.fixtrading.silverflash.Session#getSessionId()
     */
    public UUID getSessionId() {
      return sessionId;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.fixtrading.silverflash.Session#open()
     */
    public void open() throws IOException {
      // TODO Auto-generated method stub

    }

  }

  class TestReceiver implements MessageConsumer<UUID> {
    int bytesReceived = 0;
    int messagesReceived = 0;
    private byte[] dst = new byte[16 * 1024];
    private MessageHeaderWithFrame header = new MessageHeaderWithFrame();

    @Override
    public void accept(ByteBuffer buf, Session<UUID> session, long seqNo) {
      header.attachForDecode(buf, buf.position());
      bytesReceived += header.getBlockLength();
      buf.get(dst, MessageHeaderWithFrame.getLength(), header.getBlockLength());
      messagesReceived++;
    }

    public int getMessagesReceived() {
      return messagesReceived;
    }

    public int getBytesReceived() {
      return bytesReceived;
    }
  }

  private PipeTransport memoryTransport;
  final int messageCount = Byte.MAX_VALUE;
  private byte[][] messages;
  private final int schemaId = 13;
  private final int schemaVersion = 1;
  private TestReceiver serverReceiver;
  private Transport serverTransport;
  private IdempotentFlowReceiver streamReceiver;
  private final int templateId = 26;
  private int keepAliveInterval = 500;
  private Engine engine;

  public void encodeApplicationMessage(ByteBuffer buf, byte[] message) {
    MessageHeaderWithFrame.encode(buf, 0, message.length, templateId, schemaId, schemaVersion,
        message.length + MessageHeaderWithFrame.getLength());
    buf.put(message, 0, message.length);
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
    streamReceiver =
        new IdempotentFlowReceiver(engine.getReactor(), new TestSession(), serverReceiver,
            keepAliveInterval);
  }

  @After
  public void tearDown() {
    engine.close();
  }

  @Test
  public void implicitSequenceSend() throws Exception {

    TransportConsumer frameReceiver = new TransportConsumer() {
      private final FixpWithMessageLengthFrameSpliterator spliter =
          new FixpWithMessageLengthFrameSpliterator();

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
    IdempotentFlowSender sender =
        new IdempotentFlowSender(engine.getReactor(), uuid, clientTransport, keepAliveInterval,
            new SimplexSequencer());
    ByteBuffer buf = ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder());
    int bytesSent = 0;
    for (int i = 0; i < messageCount; ++i) {
      buf.clear();
      encodeApplicationMessage(buf, messages[i]);
      sender.send(buf);
      bytesSent += messages[i].length;
    }

    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {

    }

    assertEquals(messageCount, serverReceiver.getMessagesReceived());
    assertEquals(bytesSent, serverReceiver.getBytesReceived());
  }

  @Test
  public void duplicateSend() throws Exception {

    TransportConsumer frameReceiver = new TransportConsumer() {
      private final FixpWithMessageLengthFrameSpliterator spliter =
          new FixpWithMessageLengthFrameSpliterator();

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
    final SimplexSequencer sequencer = new SimplexSequencer();
    IdempotentFlowSender sender =
        new IdempotentFlowSender(engine.getReactor(), uuid, clientTransport, keepAliveInterval,
            sequencer);
    ByteBuffer buf = ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder());
    buf.clear();
    byte[] msg = "Hello world".getBytes();
    encodeApplicationMessage(buf, msg);
    sender.send(buf);
    buf.position(buf.limit());
    sequencer.setNextSeqNo(0);
    sender.send(buf);

    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {

    }

    assertEquals(1, serverReceiver.getMessagesReceived());
  }
}
