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

package io.fixprotocol.silverflash.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.KeyStore;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import javax.net.ssl.SSLSession;

import io.fixprotocol.silverflash.buffer.BufferSupplier;

/**
 * @author Don Mendelson
 *
 */
class TlsTcpClientTransport extends AbstractTlsChannel {

  public TlsTcpClientTransport(Selector selector, SocketChannel socketChannel, KeyStore keystore,
      KeyStore truststore, char[] storePassphrase) {
    super(selector, keystore, truststore, storePassphrase, false);
    this.socketChannel = socketChannel;
  }

  public CompletableFuture<? extends Transport> open(BufferSupplier buffers,
      TransportConsumer consumer) {
    Objects.requireNonNull(buffers);
    Objects.requireNonNull(consumer);
    this.buffers = buffers;
    this.consumer = consumer;

    CompletableFuture<TlsTcpClientTransport> future = new CompletableFuture<>();

    SSLSession session = engine.getSession();
    peerNetData = ByteBuffer.allocateDirect(session.getPacketBufferSize())
        .order(ByteOrder.nativeOrder());
    peerAppData = ByteBuffer.allocateDirect(session.getApplicationBufferSize())
        .order(ByteOrder.nativeOrder());
    netData = ByteBuffer.allocateDirect(session.getPacketBufferSize())
        .order(ByteOrder.nativeOrder());
    peerAppData.position(peerAppData.limit());
    netData.position(netData.limit());
    try {
      configureChannel();
      register(0);
      engine.beginHandshake();
      hsStatus = engine.getHandshakeStatus();
      initialHandshake = true;
      doHandshake();
      future.complete(this);
    } catch (IOException ex) {
      future.completeExceptionally(ex);
    }

    return future;
  }

}
