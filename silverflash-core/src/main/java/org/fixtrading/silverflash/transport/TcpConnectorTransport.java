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
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.fixtrading.silverflash.buffer.BufferSupplier;

/**
 * A client TCP transport
 * 
 * @author Don Mendelson
 *
 */
public class TcpConnectorTransport extends AbstractTcpChannel implements Connector {

  private final SocketAddress remoteAddress;
  private CompletableFuture<TcpConnectorTransport> future;


  /**
   * Constructs a TCP client Transport
   * 
   * @param selector event demultiplexor
   * @param remoteAddress address to connect to
   */
  public TcpConnectorTransport(Selector selector, SocketAddress remoteAddress) {
    super(selector);
    Objects.requireNonNull(remoteAddress);
    this.remoteAddress = remoteAddress;
  }

  /**
   * Constructs a TCP client Transport
   * 
   * @param dispatcher a dedicated dispatcher thread
   * @param remoteAddress address to connect to
   */
  public TcpConnectorTransport(Dispatcher dispatcher, SocketAddress remoteAddress) {
    super(dispatcher);
    Objects.requireNonNull(remoteAddress);
    this.remoteAddress = remoteAddress;
  }

  public CompletableFuture<? extends Transport> open(BufferSupplier buffers,
      TransportConsumer consumer) {
    Objects.requireNonNull(buffers);
    Objects.requireNonNull(consumer);
    this.buffers = buffers;
    this.consumer = consumer;

    future = new CompletableFuture<TcpConnectorTransport>();

    try {
      this.socketChannel = SocketChannel.open();
      configureChannel();
      socketChannel.configureBlocking(true);
      if (selector != null) {
        register(SelectionKey.OP_CONNECT);
      }

      this.socketChannel.connect(remoteAddress);

      if (this.socketChannel.isConnected()) {
        future.complete(this);
        consumer.connected();
      }
      
      if (dispatcher != null) {
        dispatcher.addTransport(this);
      }

    } catch (IOException ex) {
      future.completeExceptionally(ex);
    }

    return future;
  }

  public void readyToConnect() {
    try {
      socketChannel.finishConnect();
      socketChannel.keyFor(selector).interestOps(SelectionKey.OP_READ);
      future.complete(this);
      consumer.connected();
    } catch (IOException ex) {
      future.completeExceptionally(ex);
    }
  }

}
