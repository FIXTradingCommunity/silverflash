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

package org.fixtrading.silverflash.fixp;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.fixtrading.silverflash.ExceptionConsumer;
import org.fixtrading.silverflash.fixp.auth.ReactiveAuthenticator;
import org.fixtrading.silverflash.fixp.store.InMemoryMessageStore;
import org.fixtrading.silverflash.fixp.store.MessageStore;
import org.fixtrading.silverflash.reactor.ByteBufferDispatcher;
import org.fixtrading.silverflash.reactor.ByteBufferPayload;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.transport.IOReactor;
import org.fixtrading.silverflash.util.platform.AffinityThreadFactory;

/**
 * Provides services to run sessions. Typically this is used as a singleton, but it is possible to
 * run multiple groups of sessions with isolated services by using more than one Engine.
 * 
 * @author Don Mendelson
 *
 */
public class Engine implements AutoCloseable {

  /**
   * Collects attributes to build an Engine
   * 
   * If Engine is subclassed, then also subclass this Builder to add additional attributes
   *
   * @param <T> type of the object to build
   * @param <B> type of the builder
   */
  @SuppressWarnings("unchecked")
  public static class Builder<T extends Engine, B extends Builder<T, B>> {
    private ReactiveAuthenticator<UUID, ByteBuffer> authenticator = null;
    private ExceptionConsumer exceptionHandler;
    private int maxCore = -1;
    private int minCore = -1;
    private MessageStore store = null;

    /**
     * Build a new FixpSession object
     * 
     * @return a new session
     */
    public T build() {
      return (T) new Engine(this);
    }

    /**
     * Adds an Authenticator service to this Engine
     * 
     * @param authenticator an authenticator service
     * @return this Builder
     */
    public B withAuthenticator(ReactiveAuthenticator<UUID, ByteBuffer> authenticator) {
      this.authenticator = authenticator;
      return (B) this;
    }

    /**
     * Set a range of cores to pin
     * 
     * @param minCore first processor core number to pin, zero based index
     * @param maxCore last processor core number to pin, zero based index
     * @return this Builder
     */
    public B withCoreRange(int minCore, int maxCore) {
      this.minCore = minCore;
      this.maxCore = maxCore;
      return (B) this;
    }

    /**
     * Adds an exception handler
     * 
     * @param exceptionHandler a handler for exceptions thrown from an inner context
     * @return this Builder
     */
    public B withExceptionConsumer(ExceptionConsumer exceptionHandler) {
      this.exceptionHandler = exceptionHandler;
      return (B) this;
    }

    /**
     * Adds a MessageStore to this Engine
     * 
     * @param store a MessageStore more message recovery
     * @return this Builder
     */
    public B withMessageStore(MessageStore store) {
      this.store = store;
      return (B) this;
    }
  }

  @SuppressWarnings("rawtypes")
  public static Builder builder() {
    return new Builder();
  }

  private ReactiveAuthenticator<UUID, ByteBuffer> authenticator;
  private final EventReactor<ByteBuffer> eventReactor;
  private ExceptionConsumer exceptionConsumer = ex -> {
    System.err.println(ex);
  };
  private final ExecutorService executor;
  private final IOReactor iOReactor;
  private final AtomicBoolean isOpen = new AtomicBoolean();
  private Retransmitter retransmitter;
  private final Sessions sessions = new Sessions();
  private MessageStore store;

  private final AffinityThreadFactory threadFactory;

  /**
   * Construct this Engine
   * 
   * @param builder contains Engine attributes
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  protected Engine(Builder builder) {
    if (builder.minCore == -1 || builder.maxCore == -1) {
      this.threadFactory = new AffinityThreadFactory(true, true, "SES");
    } else {
      this.threadFactory =
          new AffinityThreadFactory(builder.minCore, builder.maxCore, true, true, "SES");
    }
    this.executor = Executors.newFixedThreadPool(1, threadFactory);

    this.iOReactor = new IOReactor(threadFactory);
    this.store = builder.store;
    this.authenticator = builder.authenticator;
    if (builder.exceptionHandler != null) {
      this.exceptionConsumer = builder.exceptionHandler;
    }
    this.eventReactor =
        EventReactor.builder().withDispatcher(new ByteBufferDispatcher())
            .withExceptionConsumer(exceptionConsumer).withExecutor(executor)
            .withPayloadAllocator(new ByteBufferPayload(2048)).withRingSize(256).build();
  }

  /**
   * Stop providing services
   */
  @Override
  public void close() {
    if (isOpen.compareAndSet(true, false)) {
      try {
        stopServices();
        executor.shutdown();
      } catch (Exception e) {
        exceptionConsumer.accept(e);
      }
      iOReactor.close();
      eventReactor.close();
    }
  }

  /**
   * Returns an IOReactor in running state
   * 
   * @return an IO reactor
   * @throws Exception if the IOReactor cannot be started
   */
  public IOReactor getIOReactor() throws Exception {
    CompletableFuture<IOReactor> future = iOReactor.open();
    return future.get();
  }

  /**
   * Returns an EventReactor in running state
   * 
   * @return a reactor
   */
  public EventReactor<ByteBuffer> getReactor() {
    return eventReactor;
  }

  /**
   * @return the store
   */
  public MessageStore getStore() {
    return store;
  }

  /**
   * @return the threadFactory
   */
  public ThreadFactory getThreadFactory() {
    return threadFactory;
  }

  /**
   * Returns a fixed thread pool using the thread factory used by this engine. Threads do not have
   * affinity with cores.
   * 
   * @param nThreads number of threads in the pool
   * @return a fixed thread pool
   */
  public ExecutorService newNonAffinityThreadPool(int nThreads) {
    return Executors.newFixedThreadPool(nThreads, this.threadFactory.nonAffinityThreadFactory());
  }

  /**
   * Create a new Thread
   * 
   * @param runnable function to run on this Thread
   * @param affinityEnabled set {@code true} if core affinity is desired
   * @param isDaemon set {@code true} to create daemon Thread
   * @return a new Thread
   */
  public Thread newThread(Runnable runnable, boolean affinityEnabled, boolean isDaemon) {
    return threadFactory.newThread(runnable, affinityEnabled, isDaemon);
  }

  /**
   * Returns a fixed thread pool using the thread factory used by this engine. Depending on
   * configuration, threads may or may not have affinity with cores
   * 
   * @param nThreads number of threads in the pool
   * @return a fixed thread pool
   */
  public ExecutorService newThreadPool(int nThreads) {
    return Executors.newFixedThreadPool(nThreads, this.threadFactory);
  }

  /**
   * Start providing services
   * 
   * @throws Exception if services cannot be started
   */
  public void open() throws Exception {
    if (isOpen.compareAndSet(false, true)) {
      CompletableFuture<? extends EventReactor<ByteBuffer>> future = eventReactor.open();
      future.get();
      startServices();
    }
  }

  private void startServices() {
    // todo: consider a dependency injection framework

    ArrayList<CompletableFuture<?>> futureList = new ArrayList<>();

    if (this.authenticator != null) {
      this.authenticator.withEventReactor(getReactor());
      futureList.add(this.authenticator.open());
    }

    // this.store = new CassandraMessageStore(storeConfiguration);

    // Set default store if not provided
    if (store == null) {
      this.store = new InMemoryMessageStore();
    }
    futureList.add(this.store.open());
    this.retransmitter = new Retransmitter(getReactor(), store, sessions);
    futureList.add(this.retransmitter.open());

    CompletableFuture<?>[] futures = new CompletableFuture<?>[futureList.size()];
    CompletableFuture.allOf(futureList.toArray(futures));
  }

  private void stopServices() throws Exception {
    if (this.authenticator != null) {
      this.authenticator.close();
    }
    if (this.store != null) {
      this.store.close();
    }
    if (this.retransmitter != null) {
      this.retransmitter.close();
    }
  }


}
