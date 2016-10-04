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

package io.fixprotocol.silverflash.reactor.bridge;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import io.fixprotocol.silverflash.Receiver;
import io.fixprotocol.silverflash.buffer.BufferSupplier;
import io.fixprotocol.silverflash.buffer.SingleBufferSupplier;
import io.fixprotocol.silverflash.fixp.messages.EventDecoder;
import io.fixprotocol.silverflash.fixp.messages.EventEncoder;
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderDecoder;
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderEncoder;
import io.fixprotocol.silverflash.frame.FrameSpliterator;
import io.fixprotocol.silverflash.frame.MessageFrameEncoder;
import io.fixprotocol.silverflash.frame.MessageLengthFrameEncoder;
import io.fixprotocol.silverflash.frame.MessageLengthFrameSpliterator;
import io.fixprotocol.silverflash.reactor.Dispatcher;
import io.fixprotocol.silverflash.reactor.EventReactor;
import io.fixprotocol.silverflash.reactor.Subscription;
import io.fixprotocol.silverflash.reactor.Topic;
import io.fixprotocol.silverflash.reactor.Topics;
import io.fixprotocol.silverflash.transport.Transport;
import io.fixprotocol.silverflash.transport.TransportConsumer;

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

    private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(2048)
        .order(ByteOrder.nativeOrder());
    private final MessageFrameEncoder frameEncoder= new MessageLengthFrameEncoder();
    private final MessageHeaderEncoder messageHeaderEncoder =  new MessageHeaderEncoder();
    private final Receiver forwarder;
    private final EventEncoder eventEncoder = new EventEncoder();
    private final MutableDirectBuffer mutableBuffer = new UnsafeBuffer(sendBuffer);
    private final DirectBuffer immutableBuffer = new UnsafeBuffer(new byte[0]);
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
      sendBuffer.clear();
      int offset = 0;
      frameEncoder.wrap(sendBuffer, offset).encodeFrameHeader();
      offset += frameEncoder.getHeaderLength();
      messageHeaderEncoder.wrap(mutableBuffer, offset);
      messageHeaderEncoder.blockLength(eventEncoder.sbeBlockLength())
          .templateId(eventEncoder.sbeTemplateId()).schemaId(eventEncoder.sbeSchemaId())
          .version(eventEncoder.sbeSchemaVersion());
      offset += messageHeaderEncoder.encodedLength();
      eventEncoder.wrap(mutableBuffer, offset);
      final byte[] topicBytes = Topics.toString(topic).getBytes();
      eventEncoder.putTopic(topicBytes, 0, topicBytes.length);
      payload.flip();
      immutableBuffer.wrap(payload);
      eventEncoder.putPayload(immutableBuffer, payload.position(), payload.remaining());
      frameEncoder.setMessageLength(offset + eventEncoder.encodedLength());
      frameEncoder.encodeFrameTrailer();

      transport.write(sendBuffer);

    }

  }

  private static Receiver theForwarder = t -> {
    // TODO Auto-generated method stub

  };

  private final BufferSupplier buffers = new SingleBufferSupplier(ByteBuffer.allocateDirect(
      16 * 1024).order(ByteOrder.nativeOrder()));

  private final FrameSpliterator frameSpliter = new MessageLengthFrameSpliterator();
  private final DirectBuffer immutableBuffer = new UnsafeBuffer(new byte[0]);
  private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();

  private final Consumer<? super ByteBuffer> inboundReceiver = new Consumer<ByteBuffer>() {

    private final EventDecoder eventDecoder = new EventDecoder();
    private final ByteBuffer payload = ByteBuffer.allocateDirect(16 * 1024).order(
        ByteOrder.nativeOrder());
    private final MutableDirectBuffer payloadBuffer = new UnsafeBuffer(payload);

    public void accept(ByteBuffer buffer) {
      immutableBuffer.wrap(buffer);
      int offset = buffer.position();
      messageHeaderDecoder.wrap(immutableBuffer, offset);
      offset += messageHeaderDecoder.encodedLength();
      eventDecoder.wrap(immutableBuffer, offset,
          eventDecoder.sbeBlockLength(), eventDecoder.sbeSchemaVersion());
      Topic topic= Topics.parse(eventDecoder.topic());
      eventDecoder.getPayload(payloadBuffer, 0, payload.capacity());

      EventReactorWithBridge.this.post(topic, payload);
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
   * @see io.fixprotocol.silverflash.reactor.EventReactor#close()
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
   * @see io.fixprotocol.silverflash.reactor.EventReactor#open()
   */
  @Override
  public CompletableFuture<? extends EventReactor<ByteBuffer>> open() {
    CompletableFuture<? extends EventReactor<ByteBuffer>> reactorFuture = super.open();
    CompletableFuture<? extends Transport> transportFuture = transport.open(buffers, transportConsumer);
    Throwable ex1 = null;
    try {
      reactorFuture.get();
    } catch (InterruptedException e1) {
      ex1 = e1;
    } catch (ExecutionException e1) {
      ex1 = e1.getCause();
    }
    Throwable ex2 = null;
    try {
      transportFuture.get();
    } catch (InterruptedException e1) {
      ex2 = e1;
    } catch (ExecutionException e1) {
      ex2 = e1.getCause();
    }

    CompletableFuture<EventReactor<ByteBuffer>> combined = new CompletableFuture<EventReactor<ByteBuffer>>();
    if (reactorFuture.isCompletedExceptionally()) {
      combined.completeExceptionally(ex1);
      close();
    } else if (transportFuture.isCompletedExceptionally()) {
      combined.completeExceptionally(ex2);
      close();
    } else {
      combined.complete(this);
    }
    return combined;
  }

  public void stopForwarding(Topic topic) {
    subscriptions.remove(topic);
  }

}
