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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Selector;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.fixtrading.silverflash.ExceptionConsumer;
import org.fixtrading.silverflash.auth.Crypto;
import org.fixtrading.silverflash.buffer.BufferSupplier;
import org.fixtrading.silverflash.buffer.SingleBufferSupplier;
import org.fixtrading.silverflash.transport.IOReactor;
import org.fixtrading.silverflash.transport.TlsTcpAcceptor;
import org.fixtrading.silverflash.transport.TlsTcpConnectorTransport;
import org.fixtrading.silverflash.transport.Transport;
import org.fixtrading.silverflash.transport.TransportConsumer;
import org.fixtrading.silverflash.util.platform.AffinityThreadFactory;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class TlsTcpTransportBenchmark {

  @Param({"1", "2", "4", "8"})
  public int batchSize;

  private class InjectorConsumer implements TransportConsumer {

    @Override
    public void accept(ByteBuffer t) {
      // do nothing
    }

    @Override
    public void connected() {
      startSignal.countDown();
    }

    @Override
    public void disconnected() {
      // do nothing
    }

  }

  private class Reflector implements TransportConsumer {

    private final Transport transport;

    public Reflector(Transport transport) {
      this.transport = transport;
    }

    @Override
    public void accept(ByteBuffer inbound) {
      inbound.rewind();
      try {
        transport.write(inbound);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void connected() {
      // do nothing

    }

    @Override
    public void disconnected() {
      // do nothing

    }

  }

  private static final InetSocketAddress serverAddress = new InetSocketAddress(
      InetAddress.getLoopbackAddress(), 7545);

  private static AffinityThreadFactory threadFactory;

  private char[] storePassphrase = "password".toCharArray();

  @Param({"128", "256", "1024"})
  public int bufferSize;

  private BufferSupplier clientBuffers;
  private TlsTcpConnectorTransport clientTransport;
  private IOReactor serverIOReactor;
  private IOReactor clientIOReactor;
  private byte[] message;
  private ByteBuffer[] srcs;
  private TlsTcpAcceptor tcpAcceptor;
  private CountDownLatch startSignal;

  private Function<Transport, Transport> transportWrapper = new Function<Transport, Transport>() {

    public Transport apply(Transport transport) {

      try {
        transport.open(
            new SingleBufferSupplier(ByteBuffer.allocateDirect(bufferSize * batchSize * 64).order(
                ByteOrder.nativeOrder())), new Reflector(transport)).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      return transport;
    }
  };

  @TearDown
  public void detroyTestEnvironment() throws IOException, InterruptedException {
    clientTransport.close();
    tcpAcceptor.close();
    serverIOReactor.close();
    clientIOReactor.close();
  }

  @Setup
  public void initTestEnvironment() throws IOException, InterruptedException, ExecutionException,
      GeneralSecurityException {
    startSignal = new CountDownLatch(1);
    message = new byte[bufferSize];
    Arrays.fill(message, (byte) 'x');
    srcs = new ByteBuffer[batchSize];
    for (int i = 0; i < batchSize; ++i) {
      srcs[i] = ByteBuffer.allocateDirect(bufferSize).order(ByteOrder.nativeOrder());
    }

    threadFactory = new AffinityThreadFactory(true, true, "benchmark");

    KeyStore ksKeys = Crypto.createKeyStore();
    Crypto
        .addKeyCertificateEntry(ksKeys, "exchange", "CN=trading, O=myorg, C=US", storePassphrase);

    KeyStore ksTrust = Crypto.createKeyStore();
    Crypto.addKeyCertificateEntry(ksTrust, "customer", "CN=Trader1, O=SomeFCM, C=US",
        storePassphrase);

    ExceptionConsumer exceptionConsumer = System.err::println;

    serverIOReactor = new IOReactor(threadFactory, exceptionConsumer);
    serverIOReactor.open().get();
    tcpAcceptor =
        createTcpAcceptor(serverIOReactor.getSelector(), serverAddress, ksKeys, ksTrust,
            storePassphrase, transportWrapper);
    tcpAcceptor.open().get();

    clientIOReactor = new IOReactor(threadFactory, exceptionConsumer);
    clientIOReactor.open().get();
    clientBuffers =
        new SingleBufferSupplier(ByteBuffer.allocate(bufferSize * batchSize * 64).order(
            ByteOrder.nativeOrder()));
    clientTransport =
        createClientTcpTransport(clientIOReactor.getSelector(), serverAddress, ksTrust, ksKeys,
            storePassphrase);
    clientTransport.open(clientBuffers, new InjectorConsumer());

    startSignal.await(1000L, TimeUnit.MILLISECONDS);
    // client gets accepted signal before server transport is fully
    // constructed
    Thread.sleep(500L);
  }

  private TlsTcpAcceptor createTcpAcceptor(Selector selector, SocketAddress localAddress,
      KeyStore keystore, KeyStore truststore, char[] storePassphrase,
      Function<Transport, ?> transportWrapper) {
    return new TlsTcpAcceptor(selector, localAddress, keystore, truststore, storePassphrase,
        transportWrapper);
  }

  private TlsTcpConnectorTransport createClientTcpTransport(Selector selector,
      InetSocketAddress remoteAddress, KeyStore ksKeys, KeyStore ksTrust, char[] storePassphrase) {
    return new TlsTcpConnectorTransport(selector, remoteAddress, ksKeys, ksTrust, storePassphrase);
  }

  @AuxCounters
  @State(Scope.Thread)
  public static class Counters {
    public int failed;
    public int succeeded;

    @Setup(Level.Iteration)
    public void clean() {
      failed = 0;
      succeeded = 0;
    }
  }

  @Benchmark
  public void inject(Counters counters) throws IOException {
    for (int i = 0; i < batchSize; i++) {
      srcs[i].clear();
      srcs[i].put(message);
    }
    long bytesWritten = clientTransport.write(srcs);
    if (bytesWritten == 0) {
      counters.failed++;
    } else {
      counters.succeeded++;
    }
  }

}
