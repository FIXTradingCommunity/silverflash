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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

/**
 * A client TCP transport
 * 
 * @author Don Mendelson
 *
 */
public class TcpConnectorTransport extends AbstractTcpChannel implements Connector {

  private final SocketAddress remoteAddress;


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
   * @param threadFactory supplies a thread for a dedicated dispatcher
   * @param remoteAddress address to connect to
   */
  public TcpConnectorTransport(Dispatcher dispatcher, SocketAddress remoteAddress) {
    super(dispatcher);
    Objects.requireNonNull(remoteAddress);
    this.remoteAddress = remoteAddress;
  }


  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.transport.Transport#open(java.util.function.Supplier,
   * org.fixtrading.silverflash.transport.TransportConsumer)
   */
  public void open(Supplier<ByteBuffer> buffers, TransportConsumer consumer) throws IOException {
    Objects.requireNonNull(buffers);
    Objects.requireNonNull(consumer);
    this.buffers = buffers;
    this.consumer = consumer;
    this.socketChannel = SocketChannel.open();

    if (selector != null) {
      register(SelectionKey.OP_CONNECT);
    }

    this.socketChannel.connect(remoteAddress);

    if (dispatcher != null) {
      dispatcher.addTransport(this);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.transport.Connector#connect()
   */
  public void readyToConnect() {
    try {
      socketChannel.finishConnect();
      socketChannel.keyFor(selector).interestOps(SelectionKey.OP_READ);
      consumer.connected();
    } catch (IOException e) {
      consumer.disconnected();
      e.printStackTrace();
    }
  }

}
