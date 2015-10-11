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

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Objects;

import org.fixtrading.silverflash.Service;
import org.fixtrading.silverflash.buffer.BufferSupplier;

/**
 * @author Donald
 *
 */
abstract class AbstractUdpTransport implements ReactiveTransport {

  protected BufferSupplier buffers;
  protected TransportConsumer consumer;
  protected Dispatcher dispatcher;
  protected Selector selector;
  protected DatagramChannel socketChannel;

  public AbstractUdpTransport(Selector selector) {
    Objects.requireNonNull(selector);
    this.selector = selector;
  }

  protected AbstractUdpTransport(Dispatcher dispatcher) {
    Objects.requireNonNull(dispatcher);
    this.dispatcher = dispatcher;
  }

  protected void addInterest(int ops) {
    SelectionKey key = socketChannel.keyFor(selector);
    if (key != null && key.isValid()) {
      key.interestOps(key.readyOps() | ops);
    }
  }

  public void close() {
    try {
      socketChannel.close();
    } catch (IOException e) {

    }
    if (dispatcher != null) {
      dispatcher.removeTransport(this);
    }
    if (consumer != null) {
      consumer.disconnected();
    }
    if (buffers instanceof Service) {
      try {
        ((Service) buffers).close();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  public void connected() {
    // Transport is connected when created
  }

  public void disconnected() {
    consumer.disconnected();
  }

  public boolean isFifo() {
    return false;
  }

  @Override
  public boolean isOpen() {
    return socketChannel.isOpen();
  }

  public boolean isReadyToRead() {
    return isOpen();
  }

  public int read() throws IOException {
    ByteBuffer buffer = buffers.get();
    buffer.clear();
    int bytesRead = socketChannel.read(buffer);
    buffers.commit();
    if (bytesRead < 0) {
      disconnected();
      socketChannel.close();
    } else {
      buffer.flip();
      consumer.accept(buffer);
    }
    return bytesRead;
  }

  public void readyToRead() {
    removeInterest(SelectionKey.OP_READ);
    try {
      final ByteBuffer buffer = buffers.get();
      buffer.clear();
      int bytesRead = socketChannel.read(buffer);
      buffers.commit();
      if (bytesRead < 0) {
        disconnected();
        socketChannel.close();
      } else {
        buffer.flip();
        consumer.accept(buffer);
        addInterest(SelectionKey.OP_READ);
      }

    } catch (IOException e) {
      disconnected();
    }
  }

  public void readyToWrite() {

  }

  protected void register(int ops) throws IOException {
    socketChannel.configureBlocking(false);
    if (selector != null) {
      socketChannel.register(selector, ops, this);
    }
  }

  protected void removeInterest(int ops) {
    SelectionKey key = socketChannel.keyFor(selector);
    if (key != null && key.isValid()) {
      key.interestOps(key.readyOps() & ~ops);
    }
  }

  public void setReceiveBufferSize(int bufferSize) throws IOException {
    socketChannel.setOption(StandardSocketOptions.SO_RCVBUF, bufferSize);
  }

  public void setSendBufferSize(int bufferSize) throws IOException {
    socketChannel.setOption(StandardSocketOptions.SO_SNDBUF, bufferSize);
  }

  /**
   * Keeps attempting to drain buffer until bytes written is zero due to slow consumer.
   */
  public int write(ByteBuffer src) throws IOException {
    src.flip();
    int totalBytesWritten = 0;
    while (src.hasRemaining()) {
      int bytesWritten = socketChannel.write(src);
      totalBytesWritten += bytesWritten;
      if (bytesWritten == 0) {
        break;
      }
    }
    return totalBytesWritten;
  }

  public long write(ByteBuffer[] srcs) throws IOException {
    int i = 0;
    for (i = 0; i < srcs.length; i++) {
      if (srcs[i] == null) {
        break;
      }
      srcs[i].flip();
    }
    int bytesWritten = 0;
    // This could block for a slow consumer - consider retries or breaking
    // session if write() returns 0
    while (i > 0 && srcs[i - 1].hasRemaining()) {
      bytesWritten += socketChannel.write(srcs, 0, i);
    }
    return bytesWritten;
  }

}