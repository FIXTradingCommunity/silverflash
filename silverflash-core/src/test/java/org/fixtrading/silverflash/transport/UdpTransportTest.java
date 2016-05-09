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

package org.fixtrading.silverflash.transport;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.fixtrading.silverflash.transport.IOReactor;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class UdpTransportTest {

  class TestReceiver implements TransportConsumer {
    private int bytesReceived = 0;
    private byte[] dst = new byte[16 * 1024];
    private boolean isConnected = false;

    @Override
    public void accept(ByteBuffer buf) {
      int bytesToReceive = buf.remaining();
      bytesReceived += bytesToReceive;
      buf.get(dst, 0, Math.min(bytesToReceive, dst.length));
    }

    @Override
    public void connected() {
      isConnected = true;
    }

    @Override
    public void disconnected() {
      isConnected = false;
    }

    public int getBytesReceived() {
      return bytesReceived;
    }

    public boolean isConnected() {
      return isConnected;
    }

  }

  private final InetSocketAddress clientAddress = new InetSocketAddress(
      InetAddress.getLoopbackAddress(), 7544);

  private Transport clientTransport;

  private IOReactor iOReactor;
  final int messageCount = Byte.MAX_VALUE;

  private byte[][] messages;
  private final InetSocketAddress serverAddress = new InetSocketAddress(
      InetAddress.getLoopbackAddress(), 7543);
  private Transport serverTransport;

  @Before
  public void setUp() throws Exception {
    messages = new byte[messageCount][];
    for (int i = 0; i < messageCount; ++i) {
      messages[i] = new byte[i];
      Arrays.fill(messages[i], (byte) i);
    }

    iOReactor = new IOReactor();
    iOReactor.open().get();
  }

  @After
  public void tearDown() {
    if (serverTransport != null) {
      serverTransport.close();
    }
    if (clientTransport != null) {
      clientTransport.close();
    }
    iOReactor.close();
  }

  @Test
  public void unicast() throws IOException, InterruptedException, ExecutionException, TimeoutException {

    final TestReceiver serverReceiver = new TestReceiver();
    serverTransport = new UdpTransport(iOReactor.getSelector(), serverAddress, clientAddress);
    BufferedTransportConsumer serverBuffers = new BufferedTransportConsumer(Executors.newFixedThreadPool(1), serverReceiver);
    serverTransport.open(
        serverBuffers,
        serverBuffers);

    clientTransport = new UdpTransport(iOReactor.getSelector(), clientAddress, serverAddress);

    TransportConsumer clientReceiver = new TransportConsumer() {

      @Override
      public void accept(ByteBuffer t) {

      }

      @Override
      public void connected() {
        
      }

      @Override
      public void disconnected() {

      }
    };

    BufferedTransportConsumer clientBuffers = new BufferedTransportConsumer(Executors.newFixedThreadPool(1), clientReceiver);
   
    clientTransport.open(
        clientBuffers,
        clientBuffers).get(1000L, TimeUnit.MILLISECONDS);

    ByteBuffer buf = ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder());
    int totalBytesSent = 0;
    for (int i = 0; i < messageCount; ++i) {
      buf.clear();
      buf.put(messages[i], 0, messages[i].length);
      int bytesSent = clientTransport.write(buf);
      assertEquals(messages[i].length, bytesSent);
      totalBytesSent += bytesSent;
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {

    }
    assertEquals(totalBytesSent, serverReceiver.getBytesReceived());
  }
 
  @Test
  public void multicast() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    NetworkInterface networkInterface = getMulticastInterface();
    if (networkInterface == null) {
      fail("No available multicast interface");
    }

    InetAddress multicastAddress = InetAddress.getByName("224.2.2.3");
    int port = 5555;
 
    final TestReceiver serverReceiver = new TestReceiver(); 
    serverTransport = new UdpMulticastTransport(iOReactor.getSelector(), multicastAddress, port, networkInterface);
    BufferedTransportConsumer serverBuffers = new BufferedTransportConsumer(Executors.newFixedThreadPool(1), serverReceiver);
    serverTransport.open(
        serverBuffers,
        serverBuffers);

    clientTransport = new UdpMulticastTransport(iOReactor.getSelector(), multicastAddress, port, networkInterface);

    TransportConsumer clientReceiver = new TransportConsumer() {

      @Override
      public void accept(ByteBuffer t) {

      }

      @Override
      public void connected() {
        
      }

      @Override
      public void disconnected() {

      }
    };

    BufferedTransportConsumer clientBuffers = new BufferedTransportConsumer(Executors.newFixedThreadPool(1), clientReceiver);
   
    clientTransport.open(
        clientBuffers,
        clientBuffers).get(1000L, TimeUnit.MILLISECONDS);

    ByteBuffer buf = ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder());
    int totalBytesSent = 0;
    for (int i = 0; i < messageCount; ++i) {
      buf.clear();
      buf.put(messages[i], 0, messages[i].length);
      int bytesSent = clientTransport.write(buf);
      assertEquals(messages[i].length, bytesSent);
      totalBytesSent += bytesSent;
    }

    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {

    }
    assertEquals(totalBytesSent, serverReceiver.getBytesReceived());
  }

  private NetworkInterface getMulticastInterface() throws SocketException {
    NetworkInterface networkInterface = null;
    Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
    while (interfaces.hasMoreElements()) {
      networkInterface = interfaces.nextElement();
      if (networkInterface.supportsMulticast()) {
        return networkInterface;
      }
    }
    
    return null;
  }

}
