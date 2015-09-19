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
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.function.Supplier;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

/**
 * Base class for TLS transports over TCP
 * 
 * @author Don Mendelson
 *
 */
abstract class AbstractTlsChannel extends AbstractTcpChannel {

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  static SSLContext createSSLContext(boolean clientMode, KeyStore keystore, KeyStore truststore,
      char[] storePassphrase) throws GeneralSecurityException {
    SSLContext sslContext = SSLContext.getInstance("TLS");

    if (clientMode) {
      TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
      tmf.init(truststore);
      sslContext.init(null, tmf.getTrustManagers(), null);

    } else {
      KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
      kmf.init(keystore, storePassphrase);
      sslContext.init(kmf.getKeyManagers(), null, null);
    }
    return sslContext;
  }

  /**
   * If an error occurs while processing a callback from the selector thread, the exception is saved
   * in this field to be thrown to the application the next time it calls a public method of this
   * class.
   */
  private IOException asynchException = null;
  private boolean closed = false;
  private boolean shutdown = false;
  private SSLEngineResult.Status status = null;
  protected final SSLEngine engine;
  protected HandshakeStatus hsStatus;

  /**
   * Set to true during the initial handshake. The initial handshake is special since no application
   * data can flow during it. Subsequent handshake are dealt with in a somewhat different way.
   */
  protected boolean initialHandshake = false;
  protected ByteBuffer netData;
  protected ByteBuffer peerAppData;
  protected ByteBuffer peerNetData;

  /**
   * Constructor
   * 
   * @param selector an IO reactor
   * @param keystore a store for keys
   * @param truststore a store for trusted keys for mutual authentication
   * @param storePassphrase a passphrase to access key stores
   * @param clientMode set to {@code true} to operate in client mode; otherwise works in server mode
   */
  public AbstractTlsChannel(Selector selector, KeyStore keystore, KeyStore truststore,
      char[] storePassphrase, boolean clientMode) {
    super(selector);

    try {
      SSLContext sslContext = createSSLContext(clientMode, keystore, truststore, storePassphrase);
      engine = sslContext.createSSLEngine();
      engine.setUseClientMode(clientMode);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    if (shutdown) {
      return;
    }
    shutdown = true;
    closed = true;
    asynchException = null;
    engine.closeOutbound();
    if (netData != null && netData.hasRemaining()) {
      return;
    } else {
      try {
        doShutdown();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    if (consumer != null) {
      consumer.disconnected();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.transport.Transport#open(java.util.function.Supplier,
   * org.fixtrading.silverflash.transport.TransportConsumer)
   */
  public void open(Supplier<ByteBuffer> buffers, TransportConsumer consumer) throws IOException {

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.transport.AbstractTcpChannel#readyToRead()
   */
  @Override
  public void readyToRead() {
    removeInterest(SelectionKey.OP_READ);

    try {
      if (initialHandshake) {
        doHandshake();
      } else {
        int bytesUnwrapped = 0;
        do {
          bytesUnwrapped = readAndUnwrap();
          if (bytesUnwrapped == -1) {
            consumer.disconnected();

          } else if (bytesUnwrapped == 0) {
            addInterest(SelectionKey.OP_READ);
          } else {
            peerAppData.flip();
            consumer.accept(peerAppData);
            addInterest(SelectionKey.OP_READ);
          }
        } while (bytesUnwrapped > 0);
      }

      if (shutdown) {
        doShutdown();
      }
    } catch (IOException e) {
      handleAsynchException(e);
      disconnected();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.transport.AbstractTcpChannel#readyToWrite()
   */
  @Override
  public void readyToWrite() {
    try {
      if (flushData()) {
        if (initialHandshake) {
          doHandshake();

        } else if (shutdown) {
          doShutdown();

        }
      }
    } catch (IOException e) {
      handleAsynchException(e);
      disconnected();
    }
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    checkChannelStillValid();
    if (initialHandshake) {
      return 0;
    }

    src.flip();

    if (netData.hasRemaining()) {
      return 0;
    }

    netData.clear();
    SSLEngineResult res = engine.wrap(src, netData);
    netData.flip();
    flushData();

    return res.bytesConsumed();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.transport.AbstractTcpChannel#write(java.nio.ByteBuffer[])
   */
  @Override
  public long write(ByteBuffer[] srcs) throws IOException {
    checkChannelStillValid();
    if (initialHandshake) {
      return 0;
    }

    int i = 0;
    for (i = 0; i < srcs.length; i++) {
      if (srcs[i] == null) {
        break;
      }
      srcs[i].flip();
    }

    if (netData.hasRemaining()) {
      return 0;
    }

    netData.clear();
    SSLEngineResult res = engine.wrap(srcs, 0, i, netData);

    netData.flip();
    flushData();

    return res.bytesConsumed();
  }

  private void checkChannelStillValid() throws IOException {
    if (closed) {
      throw new ClosedChannelException();
    }
    if (asynchException != null) {
      IOException ioe = new IOException("Asynchronous failure: " + asynchException.getMessage());
      ioe.initCause(asynchException);
      throw ioe;
    }
  }

  private void doShutdown() throws IOException {
    if (asynchException != null || engine.isOutboundDone()) {

      try {
        socketChannel.close();
      } catch (IOException e) { /* Ignore. */
      }
      return;
    }

    netData.clear();
    try {
      SSLEngineResult res = engine.wrap(EMPTY_BUFFER, netData);
    } catch (SSLException e1) {
      try {
        socketChannel.close();
      } catch (IOException e) { /* Ignore. */
      }
      return;
    }
    netData.flip();
    flushData();
  }

  /**
   * Execute delegated tasks in the main thread. These are compute intensive tasks, so there's no
   * point in scheduling them in a different thread.
   */
  private void doTasks() {
    Runnable task;
    while ((task = engine.getDelegatedTask()) != null) {
      task.run();
    }
    hsStatus = engine.getHandshakeStatus();
  }

  private void finishInitialHandshake() {
    initialHandshake = false;
    addInterest(SelectionKey.OP_READ);
    consumer.connected();
  }

  /**
   * Tries to write the data on the netData buffer to the socket. If not all data is sent, the write
   * interest is activated with the selector thread.
   * 
   * @return True if all data was sent. False otherwise.
   */
  private boolean flushData() throws IOException {
    if (!netData.hasRemaining()) {
      return true;
    }
    int written;
    try {
      written = socketChannel.write(netData);
    } catch (IOException ioe) {
      netData.position(netData.limit());
      throw ioe;
    }

    if (netData.hasRemaining()) {
      addInterest(SelectionKey.OP_WRITE);
      return false;
    } else {
      return true;
    }
  }

  private void handleAsynchException(IOException e) {
    asynchException = e;
    engine.closeOutbound();
  }

  private int readAndUnwrap() throws IOException {
    int bytesRead = socketChannel.read(peerNetData);

    if (bytesRead == -1) {
      engine.closeInbound();
      if (peerNetData.position() == 0 || status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
        return -1;
      }
    }

    peerAppData.clear();
    peerNetData.flip();

    SSLEngineResult res;
    do {
      res = engine.unwrap(peerNetData, peerAppData);
    } while (res.getStatus() == SSLEngineResult.Status.OK
        && res.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP
        && res.bytesProduced() == 0);

    if (res.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
      finishInitialHandshake();
    }

    if (peerAppData.position() == 0 && res.getStatus() == SSLEngineResult.Status.OK
        && peerNetData.hasRemaining()) {
      res = engine.unwrap(peerNetData, peerAppData);

    }

    status = res.getStatus();
    hsStatus = res.getHandshakeStatus();
    if (status == SSLEngineResult.Status.CLOSED) {
      shutdown = true;
      return -1;
    }

    peerNetData.compact();
    if (hsStatus == SSLEngineResult.HandshakeStatus.NEED_TASK
        || hsStatus == SSLEngineResult.HandshakeStatus.NEED_WRAP
        || hsStatus == SSLEngineResult.HandshakeStatus.FINISHED) {
      doHandshake();
    }

    return res.bytesProduced();
  }

  protected void doHandshake() throws IOException {
    while (true) {
      SSLEngineResult res;
      switch (hsStatus) {
        case FINISHED:
          if (initialHandshake) {
            finishInitialHandshake();
          }
          return;

        case NEED_TASK:
          doTasks();
          break;

        case NEED_UNWRAP:
          readAndUnwrap();
          if (initialHandshake && status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
            addInterest(SelectionKey.OP_READ);
          }
          return;

        case NEED_WRAP:
          if (netData.hasRemaining()) {
            return;
          }

          netData.clear();
          res = engine.wrap(EMPTY_BUFFER, netData);
          hsStatus = res.getHandshakeStatus();
          netData.flip();

          if (!flushData()) {
            return;
          }
          break;

        case NOT_HANDSHAKING:
          return;
      }
    }
  }
}
