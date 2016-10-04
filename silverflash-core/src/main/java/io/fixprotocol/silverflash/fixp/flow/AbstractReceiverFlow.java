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
package io.fixprotocol.silverflash.fixp.flow;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

import io.fixprotocol.silverflash.ExceptionConsumer;
import io.fixprotocol.silverflash.MessageConsumer;
import io.fixprotocol.silverflash.Session;
import io.fixprotocol.silverflash.fixp.SessionId;
import io.fixprotocol.silverflash.frame.MessageFrameEncoder;
import io.fixprotocol.silverflash.reactor.EventReactor;
import io.fixprotocol.silverflash.transport.Transport;

/**
 * @author Don Mendelson
 *
 */
class AbstractReceiverFlow {

  static abstract class Builder<T, B extends Builder<T, B>> implements FlowReceiverBuilder<T, B> {
    private ExceptionConsumer exceptionHandler;
    private MessageFrameEncoder frameEncoder;
    private long keepaliveInterval;
    private MessageConsumer<UUID> messageConsumer;
    private EventReactor<ByteBuffer> reactor;
    private Sequencer sequencer;
    private Session<UUID> session;
    private UUID sessionId;
    private Transport transport;

      public abstract T build();

    public B withExceptionConsumer(ExceptionConsumer exceptionHandler) {
      this.exceptionHandler = exceptionHandler;
      return (B) this;
    }

    @Override
    public B withKeepaliveInterval(long keepaliveInterval) {
      this.keepaliveInterval = keepaliveInterval;
      return (B) this;
    }

     public B withMessageConsumer(MessageConsumer<UUID> messageConsumer) {
      this.messageConsumer = messageConsumer;
      return (B) this;
    }

    /**
     * Provide a message frame encoder for messages to be sent.
     * @param frameEncoder the MessageFrameEncoder to use for sent messages
     * @return this Builder
     */
    public B withMessageFrameEncoder(MessageFrameEncoder frameEncoder) {
      this.frameEncoder = frameEncoder;
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
    
    public B withSession(Session<UUID> session) {
      this.session = session;
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
  }

  protected final ExceptionConsumer exceptionConsumer;
  protected final MessageFrameEncoder frameEncoder;
  protected final long keepaliveInterval;
  protected final MessageConsumer<UUID> messageConsumer;
  protected final EventReactor<ByteBuffer> reactor;
  protected final Sequencer sequencer;
  protected final Session<UUID> session;
  protected final UUID sessionId;
  protected final Transport transport;
  protected final byte[] uuidAsBytes;

  protected AbstractReceiverFlow(Builder<?, ?> builder) {
    Objects.requireNonNull(builder.reactor);
    Objects.requireNonNull(builder.transport);
    Objects.requireNonNull(builder.session);
    this.reactor = builder.reactor;
    this.transport = builder.transport;
    this.sequencer = builder.sequencer;
    this.keepaliveInterval = builder.keepaliveInterval;
    this.messageConsumer = builder.messageConsumer;
    this.frameEncoder = builder.frameEncoder;
    this.session = builder.session;
    this.sessionId = session.getSessionId();
    this.uuidAsBytes = SessionId.UUIDAsBytes(sessionId);
    this.exceptionConsumer = builder.exceptionHandler;
  }

}
