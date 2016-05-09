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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.fixtrading.silverflash.buffer.SingleBufferSupplier;
import org.fixtrading.silverflash.transport.IOReactor;
import org.fixtrading.silverflash.transport.PipeTransport;
import org.fixtrading.silverflash.transport.Transport;
import org.fixtrading.silverflash.transport.TransportConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PipeTransportTest {

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

  private PipeTransport memoryTransport;
  final int messageCount = Byte.MAX_VALUE;
  private byte[][] messages;
  private IOReactor iOReactor;
  private Transport serverTransport;
  private Transport clientTransport;

  @Before
  public void setUp() throws Exception {
    iOReactor = new IOReactor();
    iOReactor.open().get();

    memoryTransport = new PipeTransport(iOReactor.getSelector());

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
    iOReactor.close();
  }

  @Test
  public void testSend() throws IOException, InterruptedException, ExecutionException {
    serverTransport = memoryTransport.getServerTransport();
    TestReceiver serverReceiver = new TestReceiver();
    serverTransport.open(
        new SingleBufferSupplier(ByteBuffer.allocate(8096).order(ByteOrder.nativeOrder())),
        serverReceiver);

    clientTransport = memoryTransport.getClientTransport();
    TransportConsumer clientReceiver = new TransportConsumer() {

      @Override
      public void accept(ByteBuffer t) {}

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
        clientReceiver);

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
      Thread.sleep(1000);
    } catch (InterruptedException e) {

    }
    assertEquals(totalBytesSent, serverReceiver.getBytesReceived());
  }

}
