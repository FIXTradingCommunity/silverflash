package org.fixtrading.silverflash.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.KeyStore;
import java.util.Objects;
import java.util.function.Supplier;

import javax.net.ssl.SSLSession;

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

  public void open(Supplier<ByteBuffer> buffers, TransportConsumer consumer) throws IOException {
    Objects.requireNonNull(buffers);
    Objects.requireNonNull(consumer);
    this.buffers = buffers;
    this.consumer = consumer;
    SSLSession session = engine.getSession();
    peerNetData =
        ByteBuffer.allocateDirect(session.getPacketBufferSize()).order(ByteOrder.nativeOrder());
    peerAppData =
        ByteBuffer.allocateDirect(session.getApplicationBufferSize())
            .order(ByteOrder.nativeOrder());
    netData =
        ByteBuffer.allocateDirect(session.getPacketBufferSize()).order(ByteOrder.nativeOrder());
    peerAppData.position(peerAppData.limit());
    netData.position(netData.limit());
    register(0);
    engine.beginHandshake();
    hsStatus = engine.getHandshakeStatus();
    initialHandshake = true;
    doHandshake();
  }

}
