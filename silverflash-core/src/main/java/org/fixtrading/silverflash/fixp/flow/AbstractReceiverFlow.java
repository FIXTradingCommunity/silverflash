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
package org.fixtrading.silverflash.fixp.flow;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

import org.fixtrading.silverflash.MessageConsumer;
import org.fixtrading.silverflash.Session;
import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.fixp.flow.AbstractFlow.Builder;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.transport.Transport;

/**
 * @author Donald
 *
 */
class AbstractReceiverFlow {

  static abstract class Builder<T, B extends Builder<T, B>> implements FlowReceiverBuilder<T, B> {
    private MessageEncoder messageEncoder;
    private int keepaliveInterval;
    private EventReactor<ByteBuffer> reactor;
    private Sequencer sequencer;
    private UUID sessionId;
    private Transport transport;
    private MessageConsumer<UUID> streamReceiver;
    private Session<UUID> session;

      public abstract T build();

    @Override
    public B withKeepaliveInterval(int keepaliveInterval) {
      this.keepaliveInterval = keepaliveInterval;
      return (B) this;
    }

    @Override
    public B withMessageEncoder(MessageEncoder encoder) {
      this.messageEncoder = encoder;
      return (B) this;
    }

    @Override
    public B withReactor(EventReactor<ByteBuffer> reactor) {
      this.reactor = reactor;
      return (B) this;
    }

     @Override
    public B withSequencer(Sequencer sequencer) {
      this.sequencer = sequencer;
      return (B) this;
    }

    @Override
    public B withSessionId(UUID sessionId) {
      this.sessionId = sessionId;
      return (B) this;
    }

    @Override
    public B withTransport(Transport transport) {
      this.transport = transport;
      return (B) this;
    }
    
    public B withMessageConsumer(MessageConsumer<UUID> streamReceiver) {
      this.streamReceiver = streamReceiver;
      return (B) this;
    }
    public B withSession(Session<UUID> session) {
      this.session = session;
      return (B) this;
    }

  }

  protected final MessageEncoder messageEncoder;
  protected final int keepaliveInterval;
  protected final EventReactor<ByteBuffer> reactor;
  protected final Sequencer sequencer;
  protected final UUID sessionId;
  protected final Transport transport;
  protected final byte[] uuidAsBytes;
  protected final MessageConsumer<UUID> streamReceiver;
  protected final Session<UUID> session;

  protected AbstractReceiverFlow(Builder<?, ?> builder) {
    Objects.requireNonNull(builder.reactor);
    Objects.requireNonNull(builder.transport);
    Objects.requireNonNull(builder.streamReceiver);
    Objects.requireNonNull(builder.session);
    this.reactor = builder.reactor;
    this.transport = builder.transport;
    this.sequencer = builder.sequencer;
    this.messageEncoder = builder.messageEncoder;
    this.keepaliveInterval = builder.keepaliveInterval;
    this.streamReceiver = builder.streamReceiver;
    this.session = builder.session;
    this.sessionId = session.getSessionId();
    this.uuidAsBytes = SessionId.UUIDAsBytes(sessionId);
  }

}
