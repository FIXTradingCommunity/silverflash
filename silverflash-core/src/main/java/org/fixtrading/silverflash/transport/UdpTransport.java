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
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.fixtrading.silverflash.Service;
import org.fixtrading.silverflash.buffer.BufferSupplier;

/**
 * A datagram-oriented unicast Transport This Transport is demultiplexed by a Selector or added to a
 * dedicated dispatcher. thread
 * 
 * @author Don Mendelson
 *
 */
public class UdpTransport extends AbstractUdpTransport  {

  private final SocketAddress localAddress;
  private final SocketAddress remoteAddress;
  
  /**
   * Constructor with a dedicated dispatcher thread
   * 
   * @param dispatcher
   *          an existing thread for dispatching
   * @param localAddress
   *          local address to bind
   * @param remoteAddress
   *          remote address to connect to
   */
  public UdpTransport(Dispatcher dispatcher, SocketAddress localAddress,
      SocketAddress remoteAddress) {
    super(dispatcher);
     Objects.requireNonNull(localAddress);
    Objects.requireNonNull(remoteAddress);
    this.localAddress = localAddress;
    this.remoteAddress = remoteAddress;
  }

  /**
   * Constructor with IO events
   * 
   * @param selector
   *          event demultiplexor
   * @param localAddress
   *          local address to bind
   * @param remoteAddress
   *          remote address to connect to
   * 
   */
  public UdpTransport(Selector selector, SocketAddress localAddress, SocketAddress remoteAddress) {
    super(selector);
    Objects.requireNonNull(localAddress);
    Objects.requireNonNull(remoteAddress);
    this.localAddress = localAddress;
    this.remoteAddress = remoteAddress;
  }

  public CompletableFuture<? extends Transport> open(BufferSupplier buffers,
      TransportConsumer consumer) {
    Objects.requireNonNull(buffers);
    Objects.requireNonNull(consumer);
    this.buffers = buffers;
    this.consumer = consumer;

    CompletableFuture<UdpTransport> future = new CompletableFuture<UdpTransport>();

    try {
      this.socketChannel = DatagramChannel.open();
      register(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
      this.socketChannel.bind(localAddress);
      this.socketChannel.connect(remoteAddress);

      if (dispatcher != null) {
        dispatcher.addTransport(this);
      }
      if (buffers instanceof Service) {
        ((Service) buffers).open().get();
      }
      
      future.complete(this);
      consumer.connected();
    } catch (IOException | InterruptedException ex) {
      future.completeExceptionally(ex);
      try {
        socketChannel.close();
      } catch (IOException e) {
      }
    } catch (ExecutionException ex) {
      future.completeExceptionally(ex.getCause());
      try {
        socketChannel.close();
      } catch (IOException e) {
       }
    }

    return future;
  }
}
