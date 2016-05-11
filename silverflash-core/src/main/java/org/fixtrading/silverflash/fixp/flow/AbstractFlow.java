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
package org.fixtrading.silverflash.fixp.flow;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

import org.fixtrading.silverflash.ExceptionConsumer;
import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.transport.Transport;

abstract class AbstractFlow {

  static abstract class Builder<T, B extends Builder<T, B>> implements FlowBuilder<T, B> {
    private ExceptionConsumer exceptionHandler;
    private int keepaliveInterval;
    private MessageEncoder messageEncoder;
    private EventReactor<ByteBuffer> reactor;
    private Sequencer sequencer;
    private UUID sessionId;
    private Transport transport;

    /*
     * (non-Javadoc)
     * 
     * @see org.fixtrading.silverflash.fixp.flow.FlowBuilder#build()
     */
    public abstract T build();

    public B withExceptionConsumer(ExceptionConsumer exceptionHandler) {
      this.exceptionHandler = exceptionHandler;
      return (B) this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.fixtrading.silverflash.fixp.flow.FlowBuilder#withKeepaliveInterval(int)
     */
    @Override
    public B withKeepaliveInterval(int keepaliveInterval) {
      this.keepaliveInterval = keepaliveInterval;
      return (B) this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.fixtrading.silverflash.fixp.flow.FlowBuilder#withMessageEncoder(org.fixtrading.
     * silverflash.fixp.messages.MessageEncoder)
     */
    @Override
    public B withMessageEncoder(MessageEncoder encoder) {
      this.messageEncoder = encoder;
      return (B) this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.fixtrading.silverflash.fixp.flow.FlowBuilder#withReactor(org.fixtrading.silverflash.
     * reactor.EventReactor)
     */
    @Override
    public B withReactor(EventReactor<ByteBuffer> reactor) {
      this.reactor = reactor;
      return (B) this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.fixtrading.silverflash.fixp.flow.FlowBuilder#withSequencer(org.fixtrading.silverflash.
     * fixp.flow.Sequencer)
     */
    @Override
    public B withSequencer(Sequencer sequencer) {
      this.sequencer = sequencer;
      return (B) this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.fixtrading.silverflash.fixp.flow.FlowBuilder#withSessionId(java.util.UUID)
     */
    @Override
    public B withSessionId(UUID sessionId) {
      this.sessionId = sessionId;
      return (B) this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.fixtrading.silverflash.fixp.flow.FlowBuilder#withTransport(org.fixtrading.silverflash.
     * transport.Transport)
     */
    @Override
    public B withTransport(Transport transport) {
      this.transport = transport;
      return (B) this;
    }

  }

  protected final ExceptionConsumer exceptionHandler;
  protected final int keepaliveInterval;
  protected final MessageEncoder messageEncoder;
  protected final EventReactor<ByteBuffer> reactor;
  protected final Sequencer sequencer;
  protected final UUID sessionId;
  protected final Transport transport;
  protected final byte[] uuidAsBytes;

  protected AbstractFlow(Builder builder) {
    Objects.requireNonNull(builder.sessionId);
    Objects.requireNonNull(builder.reactor);
    Objects.requireNonNull(builder.transport);
    this.reactor = builder.reactor;
    this.sessionId = builder.sessionId;
    this.uuidAsBytes = SessionId.UUIDAsBytes(sessionId);
    this.transport = builder.transport;
    this.sequencer = builder.sequencer;
    this.messageEncoder = builder.messageEncoder;
    this.keepaliveInterval = builder.keepaliveInterval;
    this.exceptionHandler = builder.exceptionHandler;
  }
}
