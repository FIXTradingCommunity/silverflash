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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.fixtrading.silverflash.MessageConsumer;
import org.fixtrading.silverflash.Session;
import org.fixtrading.silverflash.auth.SimpleDirectory;
import org.fixtrading.silverflash.buffer.SingleBufferSupplier;
import org.fixtrading.silverflash.fixp.auth.SimpleAuthenticator;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.SbeMessageHeaderDecoder;
import org.fixtrading.silverflash.fixp.messages.SbeMessageHeaderEncoder;
import org.fixtrading.silverflash.frame.MessageLengthFrameEncoder;
import org.fixtrading.silverflash.reactor.ByteBufferDispatcher;
import org.fixtrading.silverflash.reactor.ByteBufferPayload;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.transport.Transport;
import org.fixtrading.silverflash.transport.UdpTransport;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

// not reliable in build environment
@Ignore
public class UdpSessionTest {

  class TestReceiver implements MessageConsumer<UUID> {
    int bytesReceived = 0;
    private byte[] dst = new byte[16 * 1024];

    @Override
    public void accept(ByteBuffer buf, Session<UUID> session, long seqNo) {
      int bytesToReceive = buf.remaining();
      bytesReceived += bytesToReceive;
      buf.get(dst, 0, bytesToReceive);
    }

    public int getBytesReceived() {
      return bytesReceived;
    }
  }

  private static final int schemaId = 33;

  private static final int schemaVersion = 0;
  static final byte STREAM_ID = 99;
  private static final int templateId = 22;

  private final InetSocketAddress clientAddress = new InetSocketAddress(
      InetAddress.getLoopbackAddress(), 7544);
  private Engine engine;
  private int keepAliveInterval = 500;
  private int messageCount = Byte.MAX_VALUE;
  private byte[][] messages;
  private EventReactor<ByteBuffer> reactor2;
  private final InetSocketAddress serverAddress = new InetSocketAddress(
      InetAddress.getLoopbackAddress(), 7543);
  private String userCredentials = "User1";
  private MessageLengthFrameEncoder frameEncoder;
  private SbeMessageHeaderEncoder sbeEncoder;

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

  @Test
  public void sendIdempotent() throws Exception {
    TestReceiver serverReceiver = new TestReceiver();
    Transport serverTransport = new UdpTransport(engine.getIOReactor().getSelector(), clientAddress,
        serverAddress);

    FixpSession serverSession = FixpSession.builder().withReactor(engine.getReactor())
        .withTransport(serverTransport)
        .withBufferSupplier(
            new SingleBufferSupplier(ByteBuffer.allocate(16 * 1024).order(ByteOrder.nativeOrder())))
        .withMessageConsumer(serverReceiver).withOutboundFlow(FlowType.IDEMPOTENT)
        .withOutboundKeepaliveInterval(keepAliveInterval).asServer().build();

    serverSession.open();

    Transport clientTransport = new UdpTransport(engine.getIOReactor().getSelector(), serverAddress,
        clientAddress);
    TestReceiver clientReceiver = new TestReceiver();
    UUID sessionId = SessionId.generateUUID();

    FixpSession clientSession = FixpSession.builder().withReactor(reactor2)
        .withTransport(clientTransport)
        .withBufferSupplier(
            new SingleBufferSupplier(ByteBuffer.allocate(16 * 1024).order(ByteOrder.nativeOrder())))
        .withMessageConsumer(clientReceiver).withOutboundFlow(FlowType.IDEMPOTENT)
        .withSessionId(sessionId).withClientCredentials(userCredentials.getBytes())
        .withOutboundKeepaliveInterval(keepAliveInterval).build();

    SessionReadyFuture readyFuture = new SessionReadyFuture(sessionId, reactor2);
    // Completes when transport is established or throws if IO error
    clientSession.open().get(1000, TimeUnit.MILLISECONDS);
    // Completes when FIXP session is established
    readyFuture.get(3000, TimeUnit.MILLISECONDS);

    ByteBuffer buf = ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder());
    int bytesSent = 0;
    for (int i = 0; i < messageCount; ++i) {
      buf.clear();
      bytesSent += encodeApplicationMessageWithFrame(buf, messages[i]);
      clientSession.send(buf);
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {

    }
    assertEquals(bytesSent, serverReceiver.getBytesReceived());

    SessionTerminatedFuture terminatedFuture = new SessionTerminatedFuture(sessionId, reactor2);
    clientSession.close();
    terminatedFuture.get(1000, TimeUnit.MILLISECONDS);

  }

  @Before
  public void setUp() throws Exception {
    frameEncoder = new MessageLengthFrameEncoder();
    sbeEncoder = new SbeMessageHeaderEncoder();

    SimpleDirectory directory = new SimpleDirectory();
    engine = Engine.builder().withAuthenticator(new SimpleAuthenticator().withDirectory(directory))
        .build();
    engine.open();
    // engine.getReactor().setTrace(true, "server");

    reactor2 = EventReactor.builder().withDispatcher(new ByteBufferDispatcher())
        .withPayloadAllocator(new ByteBufferPayload(2048)).build();
    reactor2.open().get();
    // reactor2.setTrace(true, "client");

    directory.add(userCredentials);

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

}
