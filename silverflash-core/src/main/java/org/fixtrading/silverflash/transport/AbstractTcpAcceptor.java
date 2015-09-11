package org.fixtrading.silverflash.transport;

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
   * @param selector an IO reactor
   * @param localAddress listen address
   * @param transportWrapper a function to invoke when a connection is accepted
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
