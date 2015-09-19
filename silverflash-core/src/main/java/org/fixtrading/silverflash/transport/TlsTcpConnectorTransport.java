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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.KeyStore;
import java.util.Objects;
import java.util.function.Supplier;

import javax.net.ssl.SSLSession;

/**
 * A client transport with TLS over TCP
 * 
 * @author Don Mendelson
 *
 */
public class TlsTcpConnectorTransport extends AbstractTlsChannel implements Connector {

  private final InetSocketAddress remoteAddress;


  /**
   * Create a client transport with TLS over TCP
   * 
   * @param selector event demultiplexor
   * @param remoteAddress address to connect to
   * @param keystore key store
   * @param truststore trusted keys
   * @param storePassphrase passpharse for key stores
   */
  public TlsTcpConnectorTransport(Selector selector, InetSocketAddress remoteAddress,
      KeyStore keystore, KeyStore truststore, char[] storePassphrase) {
    super(selector, keystore, truststore, storePassphrase, true);
    Objects.requireNonNull(remoteAddress);
    this.remoteAddress = remoteAddress;
  }

  public void open(Supplier<ByteBuffer> buffers, TransportConsumer consumer) throws IOException {
    Objects.requireNonNull(buffers);
    Objects.requireNonNull(consumer);
    this.buffers = buffers;
    this.consumer = consumer;
    this.socketChannel = SocketChannel.open();
    register(SelectionKey.OP_CONNECT);
    this.socketChannel.connect(remoteAddress);
  }

  public void readyToConnect() {
    try {
      socketChannel.finishConnect();
      SSLSession session = engine.getSession();
      peerNetData =
          ByteBuffer.allocateDirect(session.getPacketBufferSize()).order(ByteOrder.nativeOrder());
      peerAppData =
          ByteBuffer.allocateDirect(session.getApplicationBufferSize()).order(
              ByteOrder.nativeOrder());
      netData =
          ByteBuffer.allocateDirect(session.getPacketBufferSize()).order(ByteOrder.nativeOrder());
      peerAppData.position(peerAppData.limit());
      netData.position(netData.limit());
      removeInterest(SelectionKey.OP_CONNECT);
      engine.beginHandshake();
      hsStatus = engine.getHandshakeStatus();
      initialHandshake = true;
      doHandshake();

    } catch (IOException e) {
      consumer.disconnected();
      e.printStackTrace();
    }
  }


}
