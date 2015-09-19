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
import java.nio.ByteOrder;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.function.Function;

import org.fixtrading.silverflash.buffer.SingleBufferSupplier;

/**
 * A server TCP acceptor
 * 
 * @author Don Mendelson
 *
 */
public class TcpAcceptor extends AbstractTcpAcceptor {

  private static final Function<Transport, Transport> defaultInitializer =
      new Function<Transport, Transport>() {

        public Transport apply(Transport transport) {

          try {
            transport.open(
                new SingleBufferSupplier(ByteBuffer.allocateDirect(8096).order(
                    ByteOrder.nativeOrder())), transportConsumer);
            transportConsumer.connected();
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          return transport;
        }
      };

  private static TransportConsumer transportConsumer;


  /**
   * Constructs an acceptor
   * 
   * @param selector event demultiplexor
   * @param localAddress local address to listen on
   * @param transportConsumer handles accepted connections
   */
  public TcpAcceptor(Selector selector, SocketAddress localAddress,
      TransportConsumer transportConsumer) {
    super(selector, localAddress, defaultInitializer);
    TcpAcceptor.transportConsumer = transportConsumer;

  }

  public TcpAcceptor(Selector selector, SocketAddress localAddress,
      Function<Transport, ?> transportWrapper) {
    super(selector, localAddress, transportWrapper);

  }

  @Override
  protected TcpClientTransport createTransport(SocketChannel clientChannel) {
    return new TcpClientTransport(getSelector(), clientChannel);
  }

}
