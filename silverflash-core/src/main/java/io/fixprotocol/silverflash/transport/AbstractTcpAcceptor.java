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
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;

/**
 * A base class for TCP socket servers
 * 
 * @author Don Mendelson
 */
abstract class AbstractTcpAcceptor implements Acceptor {
  private final Executor executor = Executors.newSingleThreadExecutor();
  private final SocketAddress localAddress;
  private final Selector selector;
  private ServerSocketChannel serverSocketChannel;
  private final Function<Transport, ?> transportWrapper;

  /**
   * Constructor
   * 
   * @param selector
   *          an IO reactor
   * @param localAddress
   *          listen address
   * @param transportWrapper
   *          a function to invoke when a connection is accepted
   */
  protected AbstractTcpAcceptor(Selector selector, SocketAddress localAddress,
      Function<Transport, ?> transportWrapper) {
    Objects.requireNonNull(selector);
    Objects.requireNonNull(localAddress);
    Objects.requireNonNull(transportWrapper);
    this.selector = selector;
    this.localAddress = localAddress;
    this.transportWrapper = transportWrapper;
  }

  public void close() throws IOException {
    if (serverSocketChannel != null) {
      serverSocketChannel.close();
    }
  }

  @SuppressWarnings("unchecked")
  public Function<Transport, ?> getTransportWrapper() {
    return transportWrapper;
  }

  public CompletableFuture<? extends AbstractTcpAcceptor> open() {
    CompletableFuture<AbstractTcpAcceptor> future = new CompletableFuture<>();
    executor.execute(() -> {
      try {
        this.serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(localAddress);
        register();
        future.complete(this);
      } catch (IOException ex) {
        future.completeExceptionally(ex);
      }
    });
    return future;
  }

  public Transport readyToAccept() throws IOException {
    SocketChannel clientChannel = serverSocketChannel.accept();
    Transport clientTransport = createTransport(clientChannel);
    transportWrapper.apply(clientTransport);
    return clientTransport;
  }

  protected abstract Transport createTransport(SocketChannel clientChannel);

  protected Selector getSelector() {
    return selector;
  }

  private void register() throws IOException {
    serverSocketChannel.configureBlocking(false);
    serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT, this);
  }

}
