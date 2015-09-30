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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.fixtrading.silverflash.buffer.SingleBufferSupplier;
import org.fixtrading.silverflash.transport.SharedMemoryTransport;
import org.fixtrading.silverflash.transport.TransportConsumer;
import org.fixtrading.silverflash.util.platform.AffinityThreadFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SharedMemoryTransportTest {

  class TestReceiver implements TransportConsumer {
    private int bytesReceived = 0;
    private byte[] dst = new byte[0x10000000 / 1024];
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

    byte[] getLastMessage() {
      return dst;
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

  private SharedMemoryTransport clientTransport;
  private SharedMemoryTransport serverTransport;
  private AffinityThreadFactory threadFactory;
  private Dispatcher serverDispatcher;
  private Dispatcher clientDispatcher;

  @Before
  public void setUp() throws Exception {
    threadFactory = new AffinityThreadFactory(1, true, true, "SHMEMTRAN");
    serverDispatcher = new Dispatcher(threadFactory);
    serverTransport = new SharedMemoryTransport(false, true, 1, serverDispatcher);
    clientDispatcher = new Dispatcher(threadFactory);
    clientTransport = new SharedMemoryTransport(true, true, 1, clientDispatcher);

    messages = new byte[messageCount][];
    for (int i = 0; i < messageCount; ++i) {
      messages[i] = new byte[i];
      Arrays.fill(messages[i], (byte) i);
    }
  }

  @After
  public void tearDown() {
    serverTransport.close();
    clientTransport.close();
  }

  @Test
  public void testSend() throws IOException {

    TestReceiver serverReceiver = new TestReceiver();
    serverTransport.open(
        new SingleBufferSupplier(ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder())),
        serverReceiver);

    TransportConsumer clientReceiver = new TransportConsumer() {

      public void accept(ByteBuffer t) {
        // TODO Auto-generated method stub

      }

      public void connected() {
        // TODO Auto-generated method stub

      }

      public void disconnected() {
        // TODO Auto-generated method stub

      }

    };

    clientTransport.open(
        new SingleBufferSupplier(ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder())),
        clientReceiver);

    assertTrue(serverReceiver.isConnected());

    ByteBuffer buf = ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder());
    int totalBytesSent = 0;
    for (int i = 0; i < messageCount; ++i) {
      buf.clear();
      buf.put(messages[i], 0, messages[i].length);
      clientTransport.write(buf);
      int bytesSent = messages[i].length;
      totalBytesSent += bytesSent;
      assertEquals(messages[i].length, bytesSent);
    }

    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {

    }
    assertEquals(totalBytesSent, serverReceiver.getBytesReceived());
    byte[] last = serverReceiver.getLastMessage();
    // assertEquals((byte) (Byte.MAX_VALUE - 1), last[0]);
  }

  @Test
  public void reopen() throws IOException {

    TestReceiver serverReceiver = new TestReceiver();
    serverTransport.open(
        new SingleBufferSupplier(ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder())),
        serverReceiver);

    TransportConsumer clientReceiver = new TransportConsumer() {

      public void accept(ByteBuffer t) {
        // TODO Auto-generated method stub

      }

      public void connected() {
        // TODO Auto-generated method stub

      }

      public void disconnected() {
        // TODO Auto-generated method stub

      }

    };

    clientTransport.open(
        new SingleBufferSupplier(ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder())),
        clientReceiver);

    assertTrue(serverReceiver.isConnected());

    ByteBuffer buf = ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder());
    int bytesSent = 0;
    for (int i = 0; i < messageCount; ++i) {
      buf.clear();
      buf.put(messages[i], 0, messages[i].length);
      clientTransport.write(buf);
      bytesSent += messages[i].length;
    }

    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {

    }
    assertEquals(bytesSent, serverReceiver.getBytesReceived());

    serverTransport.close();
    clientTransport.close();

    serverTransport = new SharedMemoryTransport(false, false, 1, serverDispatcher);
    clientTransport = new SharedMemoryTransport(true, false, 1, clientDispatcher);

    serverTransport.open(
        new SingleBufferSupplier(ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder())),
        serverReceiver);

    assertTrue(serverReceiver.isConnected());

    clientTransport.open(
        new SingleBufferSupplier(ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder())),
        clientReceiver);

    for (int i = 0; i < messageCount; ++i) {
      buf.clear();
      buf.put(messages[i], 0, messages[i].length);
      clientTransport.write(buf);
      bytesSent += messages[i].length;
    }

    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {

    }
    assertEquals(bytesSent, serverReceiver.getBytesReceived());
    byte[] last = serverReceiver.getLastMessage();
    // assertEquals((byte) (Byte.MAX_VALUE - 1), last[0]);
  }
}
