/**
 * Copyright 2015 FIX Protocol Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.fixtrading.silverflash.transport;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Selector;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.fixtrading.silverflash.util.platform.AffinityThreadFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class UdpTransportBenchmark {

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

    private final AtomicInteger reflected = new AtomicInteger();
    private Transport transport;

    public Reflector() {}

    public Reflector(Transport transport) {
      this.transport = transport;
    }

    @Override
    public void accept(ByteBuffer inbound) {
      inbound.rewind();
      try {
        transport.write(inbound);
        reflected.incrementAndGet();
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

    /**
     * @return the transport
     */
    public Transport getTransport() {
      return transport;
    }

    /**
     * @param transport the transport to set
     */
    public void setTransport(Transport transport) {
      this.transport = transport;
    }

  }

  private static AffinityThreadFactory threadFactory;
  @Param({"1", "2", "4"})
  public int batchSize;

  @Param({"128", "256", "1024"})
  public int bufferSize;

  private final InetSocketAddress clientAddress =
      new InetSocketAddress(InetAddress.getLoopbackAddress(), 7544);
  private IOReactor clientIOReactor;
  private UdpTransport clientTransport;
  
  @Param({"true", "false"})
  public boolean isDemultiplexed;
  
  private byte[] message;
  private Reflector reflector;
  private final InetSocketAddress serverAddress =
      new InetSocketAddress(InetAddress.getLoopbackAddress(), 7543);
  private IOReactor serverIOReactor;
  private UdpTransport serverTransport;
  private ByteBuffer[] srcs;
  private CountDownLatch startSignal;
  private ExecutorService serverThreadPool;
  private ExecutorService clientThreadPool;

  private UdpTransport createClientTransport(Dispatcher dispatcher, InetSocketAddress serverAddress,
      InetSocketAddress clientAddress) {
    return new UdpTransport(dispatcher, serverAddress, clientAddress);
  }

  private UdpTransport createClientTransport(Selector selector, InetSocketAddress serverAddress,
      InetSocketAddress clientAddress) {
    return new UdpTransport(selector, serverAddress, clientAddress);
  }

  private UdpTransport createServerTransport(Dispatcher dispatcher, InetSocketAddress serverAddress,
      InetSocketAddress clientAddress) {
    return new UdpTransport(dispatcher, clientAddress, serverAddress);
  }


  private UdpTransport createServerTransport(Selector selector, InetSocketAddress serverAddress,
      InetSocketAddress clientAddress) {
    return new UdpTransport(selector, clientAddress, serverAddress);
  }

  @TearDown
  public void detroyTestEnvironment() throws Exception {
    clientTransport.close();
    serverTransport.close();
    if (serverIOReactor != null) {
      serverIOReactor.close();
    }
    if (clientIOReactor != null) {
      clientIOReactor.close();
    }
    clientThreadPool.shutdownNow();
    serverThreadPool.shutdownNow();
  }

  @Setup
  public void initTestEnvironment() throws IOException, InterruptedException, ExecutionException, TimeoutException {
    startSignal = new CountDownLatch(1);
    message = new byte[bufferSize];
    Arrays.fill(message, (byte) 'x');
    srcs = new ByteBuffer[batchSize];
    for (int i = 0; i < batchSize; ++i) {
      srcs[i] = ByteBuffer.allocateDirect(bufferSize).order(ByteOrder.nativeOrder());
    }

    threadFactory = new AffinityThreadFactory(true, true, "benchmark");

    reflector = new Reflector();

    serverThreadPool = Executors.newFixedThreadPool(1, threadFactory);
    BufferedTransportConsumer serverBuffers =
        new BufferedTransportConsumer(serverThreadPool, reflector);

    if (isDemultiplexed) {
      serverIOReactor = new IOReactor(threadFactory);
      serverIOReactor.open().get();
      serverTransport =
          createServerTransport(serverIOReactor.getSelector(), serverAddress, clientAddress);
      reflector.setTransport(serverTransport);
      serverTransport.open(serverBuffers, serverBuffers);
    } else {
      Dispatcher dispatcher = new Dispatcher(threadFactory);
      serverTransport = createServerTransport(dispatcher, serverAddress, clientAddress);
      reflector.setTransport(serverTransport);
      serverTransport.open(serverBuffers, serverBuffers);
      dispatcher.addTransport(serverTransport);
    }

    InjectorConsumer clientReceiver = new InjectorConsumer();
    clientThreadPool = Executors.newFixedThreadPool(1, threadFactory);
    BufferedTransportConsumer clientBuffers =
        new BufferedTransportConsumer(clientThreadPool, clientReceiver);

    CompletableFuture<? extends Transport> future;
    if (isDemultiplexed) {
      clientIOReactor = new IOReactor(threadFactory);
      clientIOReactor.open().get();
      clientTransport =
          createClientTransport(clientIOReactor.getSelector(), serverAddress, clientAddress);
      future = clientTransport.open(clientBuffers, clientBuffers);
    } else {
      Dispatcher dispatcher = new Dispatcher(threadFactory);
      clientTransport = createClientTransport(dispatcher, serverAddress, clientAddress);
      future = clientTransport.open(clientBuffers, clientBuffers);
      dispatcher.addTransport(clientTransport);
    }

    future.get(1000L, TimeUnit.MILLISECONDS);
    // client gets accepted signal before server transport is fully constructed
    Thread.sleep(500L);
  }

  @Benchmark
  public void inject() throws IOException {
    for (int i = 0; i < batchSize; i++) {
      srcs[i].clear();
      srcs[i].put(message);
    }
    clientTransport.write(srcs);
  }
}
