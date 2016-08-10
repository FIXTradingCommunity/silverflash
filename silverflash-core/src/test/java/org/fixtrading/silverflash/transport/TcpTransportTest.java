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

package org.fixtrading.silverflash.transport;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.fixtrading.silverflash.buffer.SingleBufferSupplier;
import org.fixtrading.silverflash.transport.IOReactor;
import org.fixtrading.silverflash.transport.TcpAcceptor;
import org.fixtrading.silverflash.transport.TcpConnectorTransport;
import org.fixtrading.silverflash.transport.Transport;
import org.fixtrading.silverflash.transport.TransportConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TcpTransportTest {

  class TestReceiver implements TransportConsumer {
    private int bytesReceived = 0;
    private byte[] dst = new byte[16 * 1024];
    private boolean isConnected = false;

    @Override
    public void accept(ByteBuffer buf) {
      int bytesToReceive = buf.remaining();
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


  private Transport serverTransport;
  private TcpConnectorTransport clientTransport;
  private IOReactor iOReactor;

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
  public void testSend() throws IOException, InterruptedException, ExecutionException, TimeoutException {

    InetSocketAddress serverAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 7543);


    final TestReceiver serverReceiver = new TestReceiver();

    final TcpAcceptor tcpAcceptor =
        new TcpAcceptor(iOReactor.getSelector(), serverAddress, serverReceiver);
    tcpAcceptor.open().get();

    try {
      clientTransport = new TcpConnectorTransport(iOReactor.getSelector(), serverAddress);

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

      clientTransport.open(
          new SingleBufferSupplier(ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder())),
          clientReceiver).get(1000L, TimeUnit.MILLISECONDS);
      
      // client gets accepted signal before server transport is fully constructed
      Thread.sleep(500L);
      assertTrue(serverReceiver.isConnected());

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
    } finally {
      tcpAcceptor.close();
    }
  }

  @Test
  public void testSendBuffers() throws IOException, InterruptedException, ExecutionException, TimeoutException {

    InetSocketAddress serverAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 7543);


    final TestReceiver serverReceiver = new TestReceiver();

    final TcpAcceptor tcpAcceptor =
        new TcpAcceptor(iOReactor.getSelector(), serverAddress, serverReceiver);
    tcpAcceptor.open().get();

    try {
      clientTransport = new TcpConnectorTransport(iOReactor.getSelector(), serverAddress);

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

      clientTransport.open(
          new SingleBufferSupplier(ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder())),
          clientReceiver).get(1000L, TimeUnit.MILLISECONDS);
      
      // client gets accepted signal before server transport is fully constructed
      Thread.sleep(500L);
      assertTrue(serverReceiver.isConnected());

      int batchSize = 5;
      ByteBuffer[] bufs = new ByteBuffer[batchSize];
      for (int i = 0; i < batchSize; ++i) {
        bufs[i] = ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder());
      }
      long totalBytesSent = 0;

      int expectedLength = 0;
      for (int batch = 0; batch < messageCount / batchSize; ++batch) {
        expectedLength = 0;
        for (int i = 0; i < batchSize; ++i) {
          bufs[i].clear();
          int index = batch * batchSize + i;
          bufs[i].put(messages[index], 0, messages[index].length);
          expectedLength += messages[index].length;
        }

        long bytesSent = clientTransport.write(bufs);
        assertEquals(expectedLength, bytesSent);
        totalBytesSent += bytesSent;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {

      }
      assertEquals(totalBytesSent, serverReceiver.getBytesReceived());
    } finally {
      tcpAcceptor.close();
    }
  }
}
