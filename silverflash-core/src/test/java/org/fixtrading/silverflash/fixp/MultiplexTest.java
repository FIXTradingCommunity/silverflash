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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.fixtrading.silverflash.MessageConsumer;
import org.fixtrading.silverflash.Session;
import org.fixtrading.silverflash.auth.SimpleDirectory;
import org.fixtrading.silverflash.buffer.SingleBufferSupplier;
import org.fixtrading.silverflash.fixp.Engine;
import org.fixtrading.silverflash.fixp.FixpSession;
import org.fixtrading.silverflash.fixp.FixpSharedTransportAdaptor;
import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.fixp.SessionReadyFuture;
import org.fixtrading.silverflash.fixp.SessionTerminatedFuture;
import org.fixtrading.silverflash.fixp.auth.SimpleAuthenticator;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderWithFrame;
import org.fixtrading.silverflash.transport.PipeTransport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Don Mendelson
 *
 */
public class MultiplexTest {

  class TestReceiver implements MessageConsumer<UUID> {
    int bytesReceived = 0;
    private byte[] dst = new byte[16 * 1024];

    @Override
    public void accept(ByteBuffer buf, Session<UUID> session, long seqNo) {
      int bytesToReceive = buf.remaining();
      bytesReceived += bytesToReceive;
      buf.get(dst, 0, bytesToReceive);
      // System.out.format("SeqNo %d length %d\n", seqNo, bytesToReceive);
    }

    public int getBytesReceived() {
      return bytesReceived;
    }
  }

  private static final int schemaId = 33;
  private static final int schemaVersion = 0;
  private static final int templateId = 22;

  private FixpSharedTransportAdaptor clientTransport;
  private Engine clientEngine;
  private Engine serverEngine;

  private int messageCount = Byte.MAX_VALUE;
  private byte[][] messages;
  private FixpSharedTransportAdaptor serverTransport;
  private String userCredentials = "User1";

  private class ConsumerSupplier implements Supplier<MessageConsumer<UUID>> {

    private List<TestReceiver> receivers = new ArrayList<TestReceiver>();

    public MessageConsumer<UUID> get() {
      TestReceiver receiver = new TestReceiver();
      receivers.add(receiver);
      return receiver;
    }

    public List<TestReceiver> getReceivers() {
      return receivers;
    }
  }

  private ConsumerSupplier serverSupplier = new ConsumerSupplier();

  @SuppressWarnings("unchecked")
  @Test
  public void multiplex() throws IOException, InterruptedException, ExecutionException,
      TimeoutException {
    TestReceiver clientReceiver = new TestReceiver();
    UUID sessionId = SessionId.generateUUID();

    FixpSession clientSession =
        FixpSession
            .builder()
            .withReactor(clientEngine.getReactor())
            .withTransport(clientTransport, true)
            .withBufferSupplier(
                new SingleBufferSupplier(ByteBuffer.allocate(16 * 1024).order(
                    ByteOrder.nativeOrder()))).withMessageConsumer(clientReceiver)
            .withOutboundFlow(FlowType.IDEMPOTENT).withSessionId(sessionId)
            .withClientCredentials(userCredentials.getBytes()).withOutboundKeepaliveInterval(10000)
            .build();

    SessionReadyFuture future = new SessionReadyFuture(sessionId, clientEngine.getReactor());
    clientSession.open();
    future.get(3000, TimeUnit.MILLISECONDS);

    ByteBuffer buf = ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder());
    int totalBytesSent = 0;
    for (int i = 0; i < messageCount; ++i) {
      buf.clear();
      encodeApplicationMessageWithFrame(buf, messages[i]);
      final int bytesSent = buf.position();
      long seqNo = clientSession.send(buf);
      totalBytesSent += bytesSent;
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {

    }
    int bytesSent = 0;
    for (TestReceiver t : serverSupplier.getReceivers()) {
      bytesSent += t.getBytesReceived();
    }
    assertEquals(totalBytesSent, bytesSent);

    SessionTerminatedFuture future2 =
        new SessionTerminatedFuture(sessionId, clientEngine.getReactor());
    clientSession.close();
    future.get(1000, TimeUnit.MILLISECONDS);
  }

  /**
   * @throws java.lang.Exception
   */
  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    SimpleDirectory directory = new SimpleDirectory();
    serverEngine =
        Engine.builder().withAuthenticator(new SimpleAuthenticator().withDirectory(directory))
            .build();
    serverEngine.open();

    clientEngine = Engine.builder().build();
    clientEngine.open();

    directory.add(userCredentials);

    PipeTransport memoryTransport = new PipeTransport(clientEngine.getIOReactor().getSelector());
    clientTransport =
        FixpSharedTransportAdaptor
            .builder()
            .withReactor(clientEngine.getReactor())
            .withTransport(memoryTransport.getClientTransport())
            .withBufferSupplier(
                new SingleBufferSupplier(ByteBuffer.allocate(16 * 1024).order(
                    ByteOrder.nativeOrder()))).withFlowType(FlowType.IDEMPOTENT).build();

    serverTransport =
        FixpSharedTransportAdaptor
            .builder()
            .withReactor(serverEngine.getReactor())
            .withTransport(memoryTransport.getServerTransport())
            .withBufferSupplier(
                new SingleBufferSupplier(ByteBuffer.allocate(16 * 1024).order(
                    ByteOrder.nativeOrder()))).withFlowType(FlowType.IDEMPOTENT)
            .withMessageConsumerSupplier(serverSupplier).build();

    // Must open underlying transport to receive handshake, which triggers
    // session creation
    serverTransport.openUnderlyingTransport();

    messages = new byte[messageCount][];
    for (int i = 0; i < messageCount; ++i) {
      messages[i] = new byte[i];
      Arrays.fill(messages[i], (byte) i);
    }
  }

  @After
  public void tearDown() throws Exception {
    serverTransport.close();
    serverEngine.close();
    clientEngine.close();
  }

  private void encodeApplicationMessageWithFrame(ByteBuffer buf, byte[] message) {
    MessageHeaderWithFrame.encode(buf, buf.position(), message.length, templateId, schemaId,
        schemaVersion, message.length + MessageHeaderWithFrame.getLength());
    buf.put(message);
  }
}
