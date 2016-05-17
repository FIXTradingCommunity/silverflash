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

package org.fixtrading.silverflash.fixp;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.messages.MessageSessionIdentifier;
import org.fixtrading.silverflash.frame.MessageLengthFrameSpliterator;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.transport.IdentifiableTransportConsumer;
import org.fixtrading.silverflash.transport.SharedTransportDecorator;
import org.fixtrading.silverflash.transport.TransportConsumer;

/**
 * Wraps a shared Transport for multiple sessions
 * <p>
 * Received application messages are routed to sessions by session ID (UUID) carried by FIXP session
 * messages.
 * <p>
 * This class may be used with sessions in either client or server mode. A new FixpSession in server
 * mode is created on the arrival of a Negotiate message on the Transport. New sessions are
 * configured to multiplex (sends a Context message when context switching).
 * 
 * 
 * @author Don Mendelson
 *
 */
public class FixpSharedTransportAdaptor extends SharedTransportDecorator<UUID> {

  public static class Builder
      extends SharedTransportDecorator.Builder<UUID, FixpSharedTransportAdaptor, Builder> {

    public Function<UUID, IdentifiableTransportConsumer<UUID>> consumerSupplier;
    public FlowType flowType;
    public EventReactor<ByteBuffer> reactor;

    protected Builder() {
      super();
      this.withMessageFramer(new MessageLengthFrameSpliterator());
      this.withMessageIdentifer(new MessageSessionIdentifier());
    }

    /**
     * Build a new FixpSharedTransportAdaptor object
     * 
     * @return a new adaptor
     */
    public FixpSharedTransportAdaptor build() {
      return new FixpSharedTransportAdaptor(this);
    }

    public Builder withMessageConsumerSupplier(Function<UUID, IdentifiableTransportConsumer<UUID>> consumerSupplier) {
      this.consumerSupplier = consumerSupplier;
      return this;
    }

    public Builder withFlowType(FlowType flowType) {
      this.flowType = flowType;
      return this;
    }

    public Builder withReactor(EventReactor<ByteBuffer> reactor) {
      this.reactor = reactor;
      return this;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private final Consumer<? super ByteBuffer> router = new Consumer<ByteBuffer>() {

    private UUID lastId;

    /**
     * Gets session ID from message, looks up session and invokes session consumer. If a message
     * doesn't contain a session ID, then it continues to send to the last identified session until
     * the context changes.
     */
    public void accept(ByteBuffer buffer) {
      UUID uuid = getMessageIdentifier().apply(buffer);
      if (uuid != null) {
        lastId = uuid;
      }
      TransportConsumer consumer;
      if (lastId != null) {
        consumer = getConsumer(lastId);

        if (consumer == null) {
          consumer = consumerSupplier.apply(uuid);
          addSession(uuid, consumer);
        }
        
        if (consumer != null) {
          try {
            consumer.accept(buffer);
          } catch (Exception e) {
            exceptionConsumer.accept(e);
          }
        } else {
          System.out.println("Unknown sesion ID and no uninitialized session available");
        }
      }
    }

  };

  private Function<UUID, IdentifiableTransportConsumer<UUID>> consumerSupplier;

  protected FixpSharedTransportAdaptor(Builder builder) {
    super(builder);
    if (this.getMessageIdentifier() instanceof MessageSessionIdentifier) {
      this.messageIdentifier = new MessageSessionIdentifier();
    }
    this.consumerSupplier = builder.consumerSupplier;
  }

  protected Consumer<? super ByteBuffer> getRouter() {
    return router;
  }

}
