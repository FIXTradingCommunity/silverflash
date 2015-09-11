package org.fixtrading.silverflash.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.function.Supplier;

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

  public void open(Supplier<ByteBuffer> buffers, TransportConsumer consumer) throws IOException {
    Objects.requireNonNull(buffers);
    Objects.requireNonNull(consumer);
    this.buffers = buffers;
    this.consumer = consumer;
    register(SelectionKey.OP_READ);
    consumer.connected();
  }

}
