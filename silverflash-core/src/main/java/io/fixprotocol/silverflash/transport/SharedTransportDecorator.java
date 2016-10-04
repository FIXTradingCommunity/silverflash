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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.fixprotocol.silverflash.ExceptionConsumer;
import io.fixprotocol.silverflash.buffer.BufferSupplier;
import io.fixprotocol.silverflash.frame.FrameSpliterator;

/**
 * Allows multiple sessions to share a Transport.
 * <p>
 * Received messages are routed to sessions by session ID.
 * <p>
 * The Transport is not closed until all users close their reference to the Transport. Users of the
 * Transport are required to call {@link Transport#open(BufferSupplier, TransportConsumer)} and {@link Transport#close()}
 * exactly one time each, as they would for a non-shared Transport, and they must not attempt to
 * send on the transport after closing.
 * 
 * @author Don Mendelson
 *
 * @param T identifier type
 */
public class SharedTransportDecorator<T> implements Transport, IdentifiableTransportConsumer<T> {

  /**
   * Collects attributes to build an SharedTransportDecorator
   * 
   * If SharedTransportDecorator is subclassed, then also subclass this Builder to add additional
   * attributes
   *
   * @param <T> type of the object to build
   * @param <B> type of the builder
   */
  @SuppressWarnings("unchecked")
  public static class Builder<T, U extends SharedTransportDecorator<T>, B extends Builder<T, U, B>> {

    private BufferSupplier buffers;
    private ExceptionConsumer exceptionHandler;
    private FrameSpliterator frameSpliter;
    private Function<ByteBuffer, T> messageIdentifier;
    private Transport transport;

    /**
     * Build a new SharedTransportDecorator object
     * 
     * @return a new session
     */
    public U build() {
      return (U) new SharedTransportDecorator<T>(this);
    }

    /**
     * Provide a buffer Supplier for received messages
     * 
     * @param buffers a buffer Supplier
     * @return this Builder
     */
    public B withBufferSupplier(BufferSupplier buffers) {
      this.buffers = buffers;
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
     * Provide a message framer. If not provided, a default implementation is used.
     * 
     * @param frameSpliter the frameSpliter to set
     * @return this transport
     */
    public B withMessageFramer(FrameSpliterator frameSpliter) {
      this.frameSpliter = frameSpliter;
      return (B) this;
    }

    public B withMessageIdentifer(Function<ByteBuffer, T> messageIdentifier) {
      this.messageIdentifier = messageIdentifier;
      return (B) this;
    }

    /*
     * Provide a Transport
     * 
     * @param transport a Transport to decorate
     * 
     * @return this transport
     */
    public B withTransport(Transport transport) {
      this.transport = transport;
      return (B) this;
    }
  }


  @SuppressWarnings("rawtypes")
  public static Builder builder() {
    return new Builder();
  }


  private final BufferSupplier buffers;

  private final Map<T, TransportConsumer> consumerMap = new ConcurrentHashMap<>();
  private final AtomicBoolean criticalSection = new AtomicBoolean();
  private FrameSpliterator frameSpliter;
  protected Function<ByteBuffer, T> messageIdentifier;
  private final AtomicInteger openCount = new AtomicInteger();

  private final Consumer<? super ByteBuffer> router = new Consumer<ByteBuffer>() {

    private T lastId;

    /**
     * Gets session ID from message, looks up session and invokes session consumer. If a message
     * doesn't contain a session ID, then it continues to send to the last identified session until
     * the context changes.
     */
    public void accept(ByteBuffer buffer) {
      T id = getMessageIdentifier().apply(buffer);
      if (id != null) {
        lastId = id;
      }
      if (lastId != null) {
        TransportConsumer consumer = getConsumer(lastId);
        if (consumer != null) {
          consumer.accept(buffer);
        }
      }
    }

  };

  private final Transport transport;

  protected ExceptionConsumer exceptionConsumer = System.err::println;

  protected SharedTransportDecorator(Builder<T, ?, ?> builder) {
    Objects.requireNonNull(builder.transport);
    Objects.requireNonNull(builder.buffers);
    Objects.requireNonNull(builder.messageIdentifier);

    this.transport = builder.transport;
    this.buffers = builder.buffers;
    this.messageIdentifier = builder.messageIdentifier;
    if (builder.exceptionHandler != null) {
      this.exceptionConsumer = builder.exceptionHandler;
    }
    if (builder.frameSpliter != null) {
      this.frameSpliter = builder.frameSpliter;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.function.Consumer#accept(java.lang.Object)
   */
  public void accept(ByteBuffer buffer) {
    frameSpliter.wrap(buffer);
    frameSpliter.forEachRemaining(getRouter());
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.transport.Transport#close()
   */
  public void close() {
    if (openCount.decrementAndGet() == 0) {
      transport.close();
      consumerMap.clear();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.transport.TransportConsumer#connected()
   */
  public void connected() {
    // Invoked on open()
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.transport.TransportConsumer#disconnected()
   */
  public void disconnected() {
    consumerMap.forEach((t, u) -> u.disconnected());
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.transport.TransportConsumer#getSessionId()
   */
  public T getSessionId() {
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.transport.Transport#isFifo()
   */
  public boolean isFifo() {
    return transport.isFifo();
  }

  public boolean isMessageOriented() {
    return true;
 }

  @Override
  public boolean isOpen() {
    return transport.isOpen();
  }
  
  public boolean isReadyToRead() {
    return isOpen();
  }
  
  public CompletableFuture<? extends Transport> open(BufferSupplier buffers, IdentifiableTransportConsumer<T> consumer) {
    final T sessionId = consumer.getSessionId();
    addSession(sessionId, consumer);
    CompletableFuture<? extends Transport> future = openUnderlyingTransport();
    consumer.connected();
    return future;
  }

   public CompletableFuture<? extends Transport> open(BufferSupplier buffers, TransportConsumer consumer) {
    if (consumer instanceof IdentifiableTransportConsumer<?>) {
      return open(buffers, (IdentifiableTransportConsumer<T>) consumer);
    } else {
      CompletableFuture<Transport> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalArgumentException("Not instancof IdentifiableTransportConsumer"));
      return future;
    }
  }

  /**
   * Open the Transport that this decorator wraps
   * @return a future telling the result asynchronous operation
   */
  public CompletableFuture<? extends Transport> openUnderlyingTransport() {
    if (openCount.getAndIncrement() == 0) {
      return transport.open(this.buffers, this);
    } else {
      return CompletableFuture.completedFuture(transport);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.transport.Transport#read()
   */
  public int read() throws IOException {
    return transport.read();
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.transport.Transport#write(java.nio.ByteBuffer)
   */
  public int write(ByteBuffer src) throws IOException {
    while (!criticalSection.compareAndSet(false, true)) {
      Thread.yield();
    }
    try {
      return transport.write(src);
    } finally {
      criticalSection.compareAndSet(true, false);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.transport.Transport#write(java.nio.ByteBuffer[])
   */
  public long write(ByteBuffer[] srcs) throws IOException {
    while (!criticalSection.compareAndSet(false, true)) {
      Thread.yield();
    }
    try {
      return transport.write(srcs);
    } finally {
      criticalSection.compareAndSet(true, false);
    }
  }

  /**
   * @param id identifier
   * @return message consumer
   */
  protected TransportConsumer getConsumer(T id) {
    return consumerMap.get(id);
  }

  /**
   * @return the messageIdentifier
   */
  protected Function<ByteBuffer, T> getMessageIdentifier() {
    return messageIdentifier;
  }

  /**
   * @return the router
   */
  protected Consumer<? super ByteBuffer> getRouter() {
    return router;
  }

  protected void open(Supplier<ByteBuffer> buffers, TransportConsumer consumer, T sessionId)
      throws IOException, InterruptedException, ExecutionException {
    boolean firstOpen = consumerMap.isEmpty();
    addSession(sessionId, consumer);
    if (firstOpen) {
      openUnderlyingTransport();
    }
    consumer.connected();
  }

  protected void addSession(T sessionId, TransportConsumer consumer) {
    consumerMap.put(sessionId, consumer);
  }

}
