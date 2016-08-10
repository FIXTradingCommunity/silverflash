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

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.fixtrading.silverflash.buffer.BufferSupplier;

/**
 * @author Don Mendelson
 *
 */
class TcpClientTransport extends AbstractTcpChannel {

  /**
   * @param selector event demultiplexor
   * @param socketChannel accepted channel
   */
  TcpClientTransport(Selector selector, SocketChannel socketChannel) {
    super(selector);
    this.socketChannel = socketChannel;
  }

  public CompletableFuture<? extends Transport> open(BufferSupplier buffers,
      TransportConsumer consumer) {
    Objects.requireNonNull(buffers);
    Objects.requireNonNull(consumer);
    this.buffers = buffers;
    this.consumer = consumer;

    CompletableFuture<TcpClientTransport> future = new CompletableFuture<>();

    try {
      configureChannel();
      register(SelectionKey.OP_READ);
      consumer.connected();
      future.complete(this);
    } catch (IOException ex) {
      future.completeExceptionally(ex);
    }

    return future;
  }

}
