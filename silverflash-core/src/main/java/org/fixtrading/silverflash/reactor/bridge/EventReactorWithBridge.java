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

package org.fixtrading.silverflash.reactor.bridge;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.fixtrading.silverflash.ExceptionConsumer;
import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.buffer.FrameSpliterator;
import org.fixtrading.silverflash.buffer.SingleBufferSupplier;
import org.fixtrading.silverflash.fixp.frame.FixpWithMessageLengthFrameSpliterator;
import org.fixtrading.silverflash.reactor.Dispatcher;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.Topic;
import org.fixtrading.silverflash.transport.Transport;
import org.fixtrading.silverflash.transport.TransportConsumer;

/**
 * Bidirectional forwarder between EventReactor instances. A Transport is used as a bridge, allowing
 * EventReactors to be remote.
 * 
 * @author Don Mendelson
 *
 */
public class EventReactorWithBridge extends EventReactor<ByteBuffer> {

  public static class Builder extends
      EventReactor.Builder<ByteBuffer, EventReactor<ByteBuffer>, Builder> {

    private Transport transport;

    /**
     * Build a new FixpSession object
     * 
     * @return a new session
     */
    public EventReactorWithBridge build() {
      return new EventReactorWithBridge(this);
    }

    /**
     * Adds a Transport for communications between EventReactor instances
     * 
     * @param transport a communication channel for messages
     * @return this Builder
     */
    public Builder withTransport(Transport transport) {
      this.transport = transport;
      this.withDispatcher(new ForwardDispatcher(transport, theForwarder));
      return this;
    }
  }

  private static class ForwardDispatcher implements Dispatcher<ByteBuffer> {

    private final ByteBuffer buffer = ByteBuffer.allocateDirect(2048)
        .order(ByteOrder.nativeOrder());
    private final Receiver forwarder;
    private final EventMessage message = new EventMessage();
    private final Transport transport;

    public ForwardDispatcher(Transport transport, Receiver forwarder) {
      this.transport = transport;
      this.forwarder = forwarder;
    }

    public void dispatch(Topic topic, ByteBuffer payload, Receiver receiver) throws IOException {
      // Local or remote? compare by identity
      if (receiver == forwarder) {
        forward(topic, payload);
      } else {
        payload.flip();
        receiver.accept(payload);
      }
    }

    private void forward(Topic topic, ByteBuffer payload) throws IOException {
      buffer.clear();
      message.attachForEncode(buffer, 0);
      message.setTopic(topic);
      payload.flip();
      message.setPayload(payload);
      transport.write(buffer);
    }

  }

  private static Receiver theForwarder = new Receiver() {

    public void accept(ByteBuffer t) {
      // TODO Auto-generated method stub

    }

  };

  private final Supplier<ByteBuffer> buffers = new SingleBufferSupplier(ByteBuffer.allocateDirect(
      16 * 1024).order(ByteOrder.nativeOrder()));

  private ExceptionConsumer exceptionConsumer = ex -> {
    System.err.println(ex);
  };
  private final FrameSpliterator frameSpliter = new FixpWithMessageLengthFrameSpliterator();

  private final Consumer<? super ByteBuffer> inboundReceiver = new Consumer<ByteBuffer>() {

    private final EventMessage message = new EventMessage();
    private final ByteBuffer payload = ByteBuffer.allocateDirect(16 * 1024).order(
        ByteOrder.nativeOrder());

    public void accept(ByteBuffer buffer) {
      message.attachForDecode(buffer, 0);
      Topic topic;
      try {
        topic = message.getTopic();
        message.getPayload(payload);

        EventReactorWithBridge.this.post(topic, payload);
      } catch (UnsupportedEncodingException e) {
        exceptionConsumer.accept(e);
      }
    }
  };

  private final Map<Topic, Subscription> subscriptions = new HashMap<>();

  private final Transport transport;

  private final TransportConsumer transportConsumer = new TransportConsumer() {

    public void accept(ByteBuffer buffer) {
      frameSpliter.wrap(buffer);
      frameSpliter.forEachRemaining(inboundReceiver);
    }

    public void connected() {
      // TODO Auto-generated method stub

    }

    public void disconnected() {
      // TODO Auto-generated method stub

    }

  };

  public static Builder builder() {
    return new Builder();
  }

  protected EventReactorWithBridge(Builder builder) {
    super(builder);
    this.transport = builder.transport;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.function.Consumer#accept(java.lang.Object)
   */
  public void accept(ByteBuffer t) {
    // dispatch() handles the event; Receive interface just used as marker
    // for subscription
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.reactor.EventReactor#close()
   */
  @Override
  public void close() {
    super.close();
    transport.close();
  }

  public void forward(Topic topic) {
    Subscription subscription = subscribe(topic, theForwarder);
    if (subscription != null) {
      subscriptions.put(topic, subscription);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.reactor.EventReactor#open()
   */
  @Override
  public CompletableFuture<? extends EventReactor<ByteBuffer>> open() {
    CompletableFuture<EventReactorWithBridge> future = new CompletableFuture<>();
    CompletableFuture<? extends EventReactor<ByteBuffer>> baseFuture = super.open();
    try {
      baseFuture.get();
      transport.open(buffers, transportConsumer);
      future.complete(this);
    } catch (InterruptedException | ExecutionException | IOException ex) {
      future.completeExceptionally(ex);
    }
    return future;
  }

  public void stopForwarding(Topic topic) {
    subscriptions.remove(topic);
  }

}
