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

package io.fixprotocol.silverflash.fixp;

import static org.junit.Assert.assertEquals;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.fixprotocol.silverflash.MessageConsumer;
import io.fixprotocol.silverflash.Session;
import io.fixprotocol.silverflash.auth.Crypto;
import io.fixprotocol.silverflash.auth.SimpleDirectory;
import io.fixprotocol.silverflash.buffer.SingleBufferSupplier;
import io.fixprotocol.silverflash.fixp.Engine;
import io.fixprotocol.silverflash.fixp.FixpSession;
import io.fixprotocol.silverflash.fixp.SessionId;
import io.fixprotocol.silverflash.fixp.SessionReadyFuture;
import io.fixprotocol.silverflash.fixp.SessionTerminatedFuture;
import io.fixprotocol.silverflash.fixp.auth.SimpleAuthenticator;
import io.fixprotocol.silverflash.fixp.messages.FlowType;
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderEncoder;
import io.fixprotocol.silverflash.frame.MessageLengthFrameEncoder;
import io.fixprotocol.silverflash.reactor.ByteBufferDispatcher;
import io.fixprotocol.silverflash.reactor.ByteBufferPayload;
import io.fixprotocol.silverflash.reactor.EventReactor;
import io.fixprotocol.silverflash.transport.TlsTcpAcceptor;
import io.fixprotocol.silverflash.transport.TlsTcpConnectorTransport;
import io.fixprotocol.silverflash.transport.Transport;

public class TlsTcpSessionTest {

  class TestReceiver implements MessageConsumer<UUID> {
    int bytesReceived = 0;
    private byte[] dst = new byte[16 * 1024];
    int msgsReceived = 0;

    public int getMsgsReceived() {
      return msgsReceived;
    }

    public int getBytesReceived() {
      return bytesReceived;
    }

    @Override
    public void accept(ByteBuffer buf, Session<UUID> session, long seqNo) {
      int bytesToReceive = buf.remaining();
      bytesReceived += bytesToReceive;
      buf.get(dst, 0, bytesToReceive);
      msgsReceived ++;
    }
  }

  static final byte STREAM_ID = 99;

  private static final int templateId = 22;
  private static final int schemaVersion = 0;
  private static final int schemaId = 33;

  private Engine engine;
  private EventReactor<ByteBuffer> reactor2;
  private int messageCount = Byte.MAX_VALUE;
  private byte[][] messages;
  private int keepAliveInterval = 500;
  private char[] storePassphrase = "password".toCharArray();
  private String userCredentials = "User1";
  private MessageLengthFrameEncoder frameEncoder;
  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private MutableDirectBuffer mutableBuffer = new UnsafeBuffer(new byte[0]);

  @Before
  public void setUp() throws Exception {
    frameEncoder = new MessageLengthFrameEncoder();

    SimpleDirectory directory = new SimpleDirectory();
    engine =
        Engine.builder().withAuthenticator(new SimpleAuthenticator().withDirectory(directory))
            .build();
    engine.open();

    reactor2 =
        EventReactor.builder().withDispatcher(new ByteBufferDispatcher())
            .withPayloadAllocator(new ByteBufferPayload(2048)).build();
    reactor2.open().get();

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

  @Test
  public void sendIdempotent() throws Exception {
    TestReceiver serverReceiver = new TestReceiver();
    Function<Transport, FixpSession> clientAcceptor = new Function<Transport, FixpSession>() {

      public FixpSession apply(Transport serverTransport) {
        try {
          FixpSession serverSession =
              FixpSession
                  .builder()
                  .withReactor(engine.getReactor())
                  .withTransport(serverTransport)
                  .withBufferSupplier(
                      new SingleBufferSupplier(ByteBuffer.allocate(16 * 1024).order(
                          ByteOrder.nativeOrder()))).withMessageConsumer(serverReceiver)
                  .withOutboundFlow(FlowType.Idempotent)
                  .withOutboundKeepaliveInterval(keepAliveInterval).asServer().build();

          serverSession.open();

          return serverSession;
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        return null;
      }
    };

    KeyStore ksKeys = Crypto.createKeyStore();
    Crypto
        .addKeyCertificateEntry(ksKeys, "exchange", "CN=trading, O=myorg, C=US", storePassphrase);

    KeyStore ksTrust = Crypto.createKeyStore();
    Crypto.addKeyCertificateEntry(ksTrust, "customer", "CN=Trader1, O=SomeFCM, C=US",
        storePassphrase);

    final InetSocketAddress serverAddress =
        new InetSocketAddress(InetAddress.getLoopbackAddress(), 7741);

    try (TlsTcpAcceptor tcpAcceptor =
        new TlsTcpAcceptor(engine.getIOReactor().getSelector(), serverAddress, ksKeys, ksTrust,
            storePassphrase, clientAcceptor)) {
      tcpAcceptor.open().get();

      Transport clientTransport =
          new TlsTcpConnectorTransport(engine.getIOReactor().getSelector(), serverAddress, ksTrust,
              ksKeys, storePassphrase);
      TestReceiver clientReceiver = new TestReceiver();
      UUID sessionId = SessionId.generateUUID();

      FixpSession clientSession =
          FixpSession
              .builder()
              .withReactor(reactor2)
              .withTransport(clientTransport)
              .withBufferSupplier(
                  new SingleBufferSupplier(ByteBuffer.allocate(16 * 1024).order(
                      ByteOrder.nativeOrder()))).withMessageConsumer(clientReceiver)
              .withOutboundFlow(FlowType.Idempotent).withSessionId(sessionId)
              .withClientCredentials(userCredentials.getBytes())
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
      assertEquals(messageCount, serverReceiver.getMsgsReceived());

      SessionTerminatedFuture terminatedFuture = new SessionTerminatedFuture(sessionId, reactor2);
      clientSession.close();
      terminatedFuture.get(1000, TimeUnit.MILLISECONDS);
    }
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
