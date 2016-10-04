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

package io.fixprotocol.silverflash.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.fixprotocol.silverflash.auth.Crypto;
import io.fixprotocol.silverflash.buffer.SingleBufferSupplier;
import io.fixprotocol.silverflash.transport.IOReactor;
import io.fixprotocol.silverflash.transport.TlsTcpAcceptor;
import io.fixprotocol.silverflash.transport.TlsTcpConnectorTransport;
import io.fixprotocol.silverflash.transport.Transport;
import io.fixprotocol.silverflash.transport.TransportConsumer;

public class TlsTcpTransportTest {

  class TestReceiver implements TransportConsumer {
    private int bytesReceived = 0;
    private byte[] dst = new byte[16 * 1024];
    private boolean isConnected = false;

    @Override
    public void accept(ByteBuffer buf) {
      int bytesToReceive = buf.remaining();
      // System.out.println("Application bytes received = " + bytesToReceive);
      bytesReceived += bytesToReceive;
      buf.get(dst, 0, bytesToReceive);
    }

    public int getBytesReceived() {
      return bytesReceived;
    }

    @Override
    public void connected() {
      isConnected = true;
    }

    @Override
    public void disconnected() {
      isConnected = false;
    }

    public boolean isConnected() {
      return isConnected;
    }

  }

  final int messageCount = Byte.MAX_VALUE;
  private byte[][] messages;
  private IOReactor iOReactor;

  private Thread reactorThread;
  private Transport serverTransport;
  private Transport connectorTransport;
  private char[] storePassphrase = "password".toCharArray();

  @Before
  public void setUp() throws Exception {
    iOReactor = new IOReactor();
    iOReactor.open().get();

    messages = new byte[messageCount][];
    for (int i = 0; i < messageCount; ++i) {
      messages[i] = new byte[i];
      Arrays.fill(messages[i], (byte) i);
    }
  }

  @After
  public void tearDown() {
    if (serverTransport != null) {
      serverTransport.close();
    }
    if (connectorTransport != null) {
      connectorTransport.close();
    }
    iOReactor.close();
  }

  @Test
  public void testSend() throws IOException, GeneralSecurityException, InterruptedException,
      ExecutionException, TimeoutException {

    final InetAddress localHost = InetAddress.getLocalHost();
    boolean isLoopback = localHost.isLoopbackAddress();
    InetSocketAddress serverAddress = new InetSocketAddress(localHost, 7654);
    final TestReceiver serverReceiver = new TestReceiver();

    // KeyStore ksKeys = Crypto.loadKeyStore(new FileInputStream("keystore.ks"), storePassphrase);
    KeyStore ksKeys = Crypto.createKeyStore();
    Crypto
        .addKeyCertificateEntry(ksKeys, "exchange", "CN=trading, O=myorg, C=US", storePassphrase);

    KeyStore ksTrust = Crypto.createKeyStore();
    Crypto.addKeyCertificateEntry(ksTrust, "customer", "CN=Trader1, O=SomeFCM, C=US",
        storePassphrase);

    try (TlsTcpAcceptor tcpAcceptor =
        new TlsTcpAcceptor(iOReactor.getSelector(), serverAddress, serverReceiver, ksKeys, ksTrust,
            storePassphrase)) {
      tcpAcceptor.open().get();

      connectorTransport =
          new TlsTcpConnectorTransport(iOReactor.getSelector(), serverAddress, ksTrust, ksKeys,
              storePassphrase);
      TestReceiver clientReceiver = new TestReceiver();

      connectorTransport.open(
          new SingleBufferSupplier(ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder())),
          clientReceiver).get(1000L, TimeUnit.MILLISECONDS);

      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {

      }

      assertTrue(serverReceiver.isConnected());
      assertTrue(clientReceiver.isConnected());

      ByteBuffer buf = ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder());
      int totalBytesSent = 0;
      for (int i = 0; i < messageCount; ++i) {
        buf.clear();
        buf.put(messages[i], 0, messages[i].length);
        int bytesSent = connectorTransport.write(buf);
        assertEquals(messages[i].length, bytesSent);
        totalBytesSent += bytesSent;
      }

      try {
        Thread.sleep(1500);
      } catch (InterruptedException e) {

      }
      assertEquals(totalBytesSent, serverReceiver.getBytesReceived());
    }
  }

}
