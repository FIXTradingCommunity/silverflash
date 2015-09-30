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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

/**
 * Base class for TCP transports demultiplexed by a Selector or added to a dedicated dispatcher
 * thread
 * 
 * @author Don Mendelson
 *
 */
abstract class AbstractTcpChannel implements ReactiveTransport {


  protected Supplier<ByteBuffer> buffers;
  protected TransportConsumer consumer;
  protected Dispatcher dispatcher;
  protected Selector selector;
  protected SocketChannel socketChannel;

  /**
   * Constructor
   * 
   * @param dispatcher an existing thread for dispatching
   */
  AbstractTcpChannel(Dispatcher dispatcher) {
    Objects.requireNonNull(dispatcher);
    this.dispatcher = dispatcher;
  }

  /**
   * Constructor
   * 
   * @param selector event demultiplexor
   */
  AbstractTcpChannel(Selector selector) {
    Objects.requireNonNull(selector);
    this.selector = selector;
  }

  public void close() {
    try {
      socketChannel.close();
    } catch (IOException e) {

    }
    if (consumer != null) {
      consumer.disconnected();
    }
  }

  public void connected() {
    // Transport is connected when created
  }

  public void disconnected() {
    consumer.disconnected();
  }

  public boolean isFifo() {
    return true;
  }

  @Override
  public boolean isOpen() {
    return socketChannel.isOpen();
  }
  
  public boolean isReadyToRead() {
    return isOpen();
  }

  public void open(Supplier<ByteBuffer> buffers, TransportConsumer consumer) throws IOException {
    Objects.requireNonNull(buffers);
    Objects.requireNonNull(consumer);
    this.buffers = buffers;
    this.consumer = consumer;
  }

  public int read() throws IOException {
    ByteBuffer buffer = buffers.get();
    buffer.clear();
    int bytesRead = socketChannel.read(buffer);
    if (bytesRead > 0) {
      buffer.flip();
      consumer.accept(buffer);
    }
    return bytesRead;
  }

  public void readyToRead() {
    removeInterest(SelectionKey.OP_READ);
    int bytesRead = 0;
    try {
      final ByteBuffer buffer = buffers.get();
      buffer.clear();
      bytesRead = socketChannel.read(buffer);
      if (bytesRead < 0) {
        // Peer reset
        disconnected();
        socketChannel.close();
      } else {
        try {
          buffer.flip();
          consumer.accept(buffer);
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          addInterest(SelectionKey.OP_READ);
        }
      }
    } catch (IOException e) {
      disconnected();
    }
  }

  public void readyToWrite() {

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

  protected void addInterest(int ops) {
    SelectionKey key = socketChannel.keyFor(selector);
    if (key != null && key.isValid()) {
      key.interestOps(key.readyOps() | ops);
    }
  }

  protected void register(int ops) throws IOException {
    socketChannel.configureBlocking(false);
    socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
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


  Dispatcher getDispatcher() {
    return dispatcher;
  }
}
