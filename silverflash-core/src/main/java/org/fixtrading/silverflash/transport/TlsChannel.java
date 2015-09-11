package org.fixtrading.silverflash.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Objects;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;


/**
 * @author Don Mendelson
 *
 */
class TlsChannel {
  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  static void close(WritableByteChannel writableChannel, SSLEngine engine, ByteBuffer myNetData)
      throws IOException {
    Objects.requireNonNull(writableChannel);
    Objects.requireNonNull(engine);
    // Indicate that application is done with engine
    engine.closeOutbound();

    while (!engine.isOutboundDone()) {
      // Get close message
      SSLEngineResult res = engine.wrap(EMPTY_BUFFER, myNetData);

      // Check res statuses

      // Send close message to peer
      while (myNetData.hasRemaining()) {
        int num = writableChannel.write(myNetData);
        if (num == -1) {
          // handle closed channel
        } else if (num == 0) {
          // no bytes written; try again later
        }
        myNetData.compact();
      }
    }

    // Close transport
    writableChannel.close();
  }

  static SSLEngineResult.HandshakeStatus doHandshake(WritableByteChannel writableChannel,
      ReadableByteChannel readableChannel, SSLEngine engine, ByteBuffer myNetData,
      ByteBuffer peerNetData) throws IOException {
    Objects.requireNonNull(writableChannel);
    Objects.requireNonNull(readableChannel);
    Objects.requireNonNull(engine);

    // Create byte buffers to use for holding application data
    int appBufferSize = engine.getSession().getApplicationBufferSize();
    ByteBuffer peerAppData =
        ByteBuffer.allocateDirect(appBufferSize).order(ByteOrder.nativeOrder());

    // Begin handshake
    engine.beginHandshake();
    SSLEngineResult.HandshakeStatus hs = engine.getHandshakeStatus();

    // Process handshaking message
    while (hs != SSLEngineResult.HandshakeStatus.FINISHED
        && hs != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {

      switch (hs) {

        case NEED_UNWRAP:
          // Receive handshaking data from peer
          if (readableChannel.read(peerNetData) < 0) {
            // Handle closed channel
            return hs;
          }

          // Process incoming handshaking data
          peerNetData.flip();
          SSLEngineResult res = engine.unwrap(peerNetData, peerAppData);
          peerNetData.compact();
          hs = res.getHandshakeStatus();

          // Check status
          switch (res.getStatus()) {
            case OK:
              // Handle OK status
              break;
            case BUFFER_OVERFLOW:
              peerAppData =
                  ByteBuffer.allocate(peerAppData.capacity() + 4096).order(ByteOrder.nativeOrder());
              break;
            case BUFFER_UNDERFLOW:
              // We need to read some more data from the channel.
              break;
            case CLOSED:
              return hs;
          }
          break;

        case NEED_WRAP:
          // Empty the local network packet buffer.
          myNetData.clear();

          // Generate handshaking data
          res = engine.wrap(EMPTY_BUFFER, myNetData);
          hs = res.getHandshakeStatus();

          // Check status
          switch (res.getStatus()) {
            case OK:
              myNetData.flip();

              // Send the handshaking data to peer
              while (myNetData.hasRemaining()) {
                if (writableChannel.write(myNetData) < 0) {
                  // Handle closed channel
                  return hs;
                }
              }
              break;
            case BUFFER_OVERFLOW:
              break;
            case BUFFER_UNDERFLOW:
              break;
            case CLOSED:
              return hs;
          }
          break;

        case NEED_TASK:
          // Handle blocking tasks - todo: use Executor ?
          Runnable task;
          while ((task = engine.getDelegatedTask()) != null) {
            new Thread(task).start();
          }
          break;
        default:
          break;
      }
    }

    return hs;

  }

  /**
   * Initiates TrustManager, KeyManager and SSLEngine.
   * <p>
   * Initialized objects perform bidirectional authentication. TrustManager: Determines whether the
   * remote authentication credentials (and thus the connection) should be trusted. KeyManager:
   * Determines which authentication credentials to send to the remote host.
   * <p>
   * Truststore is stored in home directory as {@code [client | server].ts}
   * <p>
   * Keystore is stored in home directory as {@code [client | server].ks}
   * <p>
   * Command to create client keystore:
   * 
   * <pre>
   * keytool -genkeypair -dname "cn=client, o=myorg, c=US"
   *       -alias client -keypass <password> -keystore $HOME/client.ks
   *       -storepass <passphrase> -validity 365 -sigalg dsa
   * </pre>
   * 
   * @param useClientMode if {@code true} the SSLEngine uses client mode to initiate handshake.
   *        Otherwise, it operates in server mode.
   * @param remoteAddress remote IP address
   * @return initialized SSLEngine
   * @throws GeneralSecurityException if security credentials cannot be processed because they are
   *         malformed or of an unsupported type
   */
  static SSLEngine initalizeSslContext(boolean useClientMode, KeyStore ksKeys, KeyStore ksTrust,
      char[] storePassphrase, InetSocketAddress remoteAddress) throws GeneralSecurityException {
    // Create/initialize the SSLContext with key material
    // KeyManager's decide which key material to use.
    // Default algorithm is SunX509
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(ksKeys, storePassphrase);

    // TrustManager's decide whether to allow connections.
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ksTrust);

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

    // We're ready for the engine.
    SSLEngine engine =
        sslContext.createSSLEngine(remoteAddress.getHostName(), remoteAddress.getPort());

    // Use as client
    engine.setUseClientMode(useClientMode);

    return engine;
  }

  static void onRead(SSLEngine engine, ByteBuffer peerNetData, ByteBuffer peerAppData)
      throws SSLException {
    Objects.requireNonNull(engine);
    // Process incoming data
    peerNetData.flip();
    SSLEngineResult res = engine.unwrap(peerNetData, peerAppData);

    final Status hs = res.getStatus();
    switch (hs) {
      case OK:
        peerNetData.compact();

        if (peerAppData.hasRemaining()) {
          // Use peerAppData
        }
      case BUFFER_OVERFLOW:
        // Maybe need to enlarge the peer application data buffer.
        if (engine.getSession().getApplicationBufferSize() > peerAppData.capacity()) {
          // enlarge the peer application data buffer
        } else {
          // compact or clear the buffer
        }
        // retry the operation
        break;
      case BUFFER_UNDERFLOW:
        // Maybe need to enlarge the peer network packet buffer
        if (engine.getSession().getPacketBufferSize() > peerNetData.capacity()) {
          // enlarge the peer network packet buffer
        } else {
          // compact or clear the buffer
        }
        // obtain more inbound network data and then retry the operation
        break;
      case CLOSED:
        break;
    }
  }

  /**
   * 
   * @param writableChannel channel used to send
   * @param engine performs SSL operations
   * @param src source buffer
   * @param myNetData buffer holding data to send. Assumes that {@code ByteBuffer#flip()} was
   *        invoked before calling this method.
   * @return number of bytes written, which may be different than remaining length of src buffer
   * @throws IOException if an IO error occurs
   */
  static int write(WritableByteChannel writableChannel, SSLEngine engine, ByteBuffer src,
      ByteBuffer myNetData) throws IOException {
    Objects.requireNonNull(writableChannel);
    Objects.requireNonNull(engine);

    int bytesWritten = 0;

    while (src.hasRemaining()) {
      // Generate SSL/TLS encoded data (handshake or application data)
      SSLEngineResult res = engine.wrap(src, myNetData);

      // Process status of call
      final Status hs = res.getStatus();
      switch (hs) {
        case OK:
          src.compact();

          // Send SSL/TLS encoded data to peer
          while (myNetData.hasRemaining()) {
            int num = writableChannel.write(myNetData);
            if (num == -1) {
              // handle closed channel
            } else if (num == 0) {
              // no bytes written; try again later
            } else {
              bytesWritten += num;
            }
          }
        case BUFFER_OVERFLOW:
          // Increase the target buffer size

          // retry the operation
          break;
        case BUFFER_UNDERFLOW:
          break;
        case CLOSED:
          break;
      }
    }
    return bytesWritten;
  }

}
