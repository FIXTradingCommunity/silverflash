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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.channels.DatagramChannel;
import java.nio.channels.MembershipKey;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.fixtrading.silverflash.Service;
import org.fixtrading.silverflash.buffer.BufferSupplier;

/**
 * A datagram-oriented multicast Transport This Transport is demultiplexed by a Selector or added to
 * a dedicated dispatcher. thread
 * 
 * @author Don Mendelson
 *
 */
public class UdpMulticastTransport extends AbstractUdpTransport {

  private final InetAddress multicastGroup;
  private final NetworkInterface networkInterface;
  private final int port;
  private MembershipKey key;
  
  /**
   * Constructor
   * 
   * @param dispatcher
   *          an existing thread for dispatching
   * @param multicastGroup multicast address
   * @param port multicast port
   * @param networkInterface network interface
   */
  public UdpMulticastTransport(Dispatcher dispatcher, InetAddress multicastGroup, int port,
      NetworkInterface networkInterface) {
    super(dispatcher);
    Objects.requireNonNull(multicastGroup);
    Objects.requireNonNull(networkInterface);
    this.multicastGroup = multicastGroup;
    this.port = port;
    this.networkInterface = networkInterface;
  }

  /**
   * Constructor
   * 
   * @param selector
   *          event demultiplexor
   * @param multicastGroup multicast address
   * @param port multicast port
   * @param networkInterface network interface
   */
  public UdpMulticastTransport(Selector selector, InetAddress multicastGroup, int port,
      NetworkInterface networkInterface) {
    super(selector);
     Objects.requireNonNull(multicastGroup);
    Objects.requireNonNull(networkInterface);
    this.multicastGroup = multicastGroup;
    this.port = port;
    this.networkInterface = networkInterface;
  }

  public CompletableFuture<? extends Transport> open(BufferSupplier buffers,
      TransportConsumer consumer) {
    Objects.requireNonNull(buffers);
    Objects.requireNonNull(consumer);
    this.buffers = buffers;
    this.consumer = consumer;

    CompletableFuture<UdpMulticastTransport> future = new CompletableFuture<>();
    try {

      this.socketChannel = DatagramChannel.open(StandardProtocolFamily.INET);
      register(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
      this.socketChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);
      this.socketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
      InetSocketAddress local = new InetSocketAddress(port);
      this.socketChannel.bind(local);
      key = socketChannel.join(multicastGroup, networkInterface);

      if (dispatcher != null) {
        dispatcher.addTransport(this);
      }
      if (buffers instanceof Service) {
        ((Service) buffers).open().get();
      }

      consumer.connected();
      future.complete(this);
    } catch (IOException | InterruptedException ex) {
      future.completeExceptionally(ex);
    } catch (ExecutionException ex) {
      future.completeExceptionally(ex.getCause());
    }

    return future;
  }

  /* (non-Javadoc)
   * @see org.fixtrading.silverflash.transport.AbstractUdpTransport#close()
   */
  @Override
  public void close() {
    if (key != null) {
      key.drop();
    }
    super.close();
  }
}
