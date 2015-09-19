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

import static org.fixtrading.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_READY;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_SUSPENDED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.CLIENT_ESTABLISHED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.PEER_TERMINATED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.SERVER_ESTABLISHED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.SERVER_NEGOTIATED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.ToSessionEventType.APPLICATION_MESSAGE_TO_SEND;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

import org.fixtrading.silverflash.ExceptionConsumer;
import org.fixtrading.silverflash.MessageConsumer;
import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.RecoverableSender;
import org.fixtrading.silverflash.Sequenced;
import org.fixtrading.silverflash.Session;
import org.fixtrading.silverflash.buffer.FrameSpliterator;
import org.fixtrading.silverflash.fixp.flow.ClientSessionEstablisher;
import org.fixtrading.silverflash.fixp.flow.FlowReceiver;
import org.fixtrading.silverflash.fixp.flow.FlowSender;
import org.fixtrading.silverflash.fixp.flow.IdempotentFlowReceiver;
import org.fixtrading.silverflash.fixp.flow.IdempotentFlowSender;
import org.fixtrading.silverflash.fixp.flow.MultiplexSequencer;
import org.fixtrading.silverflash.fixp.flow.MutableSequence;
import org.fixtrading.silverflash.fixp.flow.NoneReceiver;
import org.fixtrading.silverflash.fixp.flow.NoneSender;
import org.fixtrading.silverflash.fixp.flow.RecoverableFlowReceiver;
import org.fixtrading.silverflash.fixp.flow.RecoverableFlowSender;
import org.fixtrading.silverflash.fixp.flow.Sequencer;
import org.fixtrading.silverflash.fixp.flow.ServerSessionEstablisher;
import org.fixtrading.silverflash.fixp.flow.SimplexSequencer;
import org.fixtrading.silverflash.fixp.flow.SimplexStreamSequencer;
import org.fixtrading.silverflash.fixp.flow.UnsequencedFlowReceiver;
import org.fixtrading.silverflash.fixp.flow.UnsequencedFlowSender;
import org.fixtrading.silverflash.fixp.frame.FixpWithMessageLengthFrameSpliterator;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.store.MessageStore;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.Topic;
import org.fixtrading.silverflash.transport.IdentifiableTransportConsumer;
import org.fixtrading.silverflash.transport.Transport;

/**
 * A Session that implements FIXP, FIX high performance session protocol
 * 
 * @author Don Mendelson
 *
 */
public class FixpSession implements Session<UUID>, RecoverableSender {

  /**
   * Collects attributes to build an FixpSession
   * 
   * If FixpSession is subclassed, then also subclass this Builder to add additional attributes
   *
   * @param <T> type of the object to build
   * @param <B> type of the builder
   */
  @SuppressWarnings("unchecked")
  public static class Builder<T extends FixpSession, B extends Builder<T, B>> {

    private Supplier<ByteBuffer> buffers;
    private byte[] credentials = null;
    private ExceptionConsumer exceptionHandler;
    private FrameSpliterator frameSpliter = null;
    private boolean isMultiplexedTransport = false;
    private boolean isServer = false;
    private MessageConsumer<UUID> messageConsumer = null;
    private FlowType outboundFlow = FlowType.IDEMPOTENT;
    private int outboundKeepaliveInterval = 10000;
    private EventReactor<ByteBuffer> reactor = null;
    private UUID sessionId = SessionId.EMPTY;
    private MessageStore store = null;
    private Transport transport = null;

    /**
     * This session will play the server role
     * 
     * @return this Builder
     */
    public B asServer() {
      this.isServer = true;
      return (B) this;
    }

    /**
     * Build a new FixpSession object
     * 
     * @return a new session
     */
    public T build() {
      return (T) new FixpSession(this);
    }

    /**
     * Provide a buffer Supplier for received messages
     * 
     * @param buffers a buffer Supplier
     * @return this Builder
     */
    public B withBufferSupplier(Supplier<ByteBuffer> buffers) {
      this.buffers = buffers;
      return (B) this;
    }

    /**
     * Provide client identification
     * 
     * @param credentials business entity identification
     * @return this Builder
     */
    public B withClientCredentials(byte[] credentials) {
      this.credentials = credentials;
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
     * 
     * @param messageConsumer application layer consumer of messages
     * @return this Builder
     */
    public B withMessageConsumer(MessageConsumer<UUID> messageConsumer) {
      this.messageConsumer = messageConsumer;
      return (B) this;
    }

    /**
     * Provide a message framer. If not provided, a default implementation is used.
     * 
     * @param frameSpliter the frameSpliter to set
     * @return this Builder
     */
    public B withMessageFramer(FrameSpliterator frameSpliter) {
      this.frameSpliter = frameSpliter;
      return (B) this;
    }

    /**
     * Provide a MessageStore for recoverable flows
     * 
     * @param store to persist messages
     * @return this Builder
     */
    public B withMessageStore(MessageStore store) {
      Objects.requireNonNull(store);
      this.store = store;
      return (B) this;
    }

    /**
     * Set outbound flow type
     * 
     * @param outboundFlow type of flow for outbound messages
     * @return this Builder
     */
    public B withOutboundFlow(FlowType outboundFlow) {
      this.outboundFlow = outboundFlow;
      return (B) this;
    }

    /**
     * Set heartbeat interval for the outbound flow. If not set, a default value is used.
     * 
     * @param outboundKeepaliveInterval interval in milliseconds
     * @return this Builder
     */
    public B withOutboundKeepaliveInterval(int outboundKeepaliveInterval) {
      this.outboundKeepaliveInterval = outboundKeepaliveInterval;
      return (B) this;
    }

    /**
     * Set an EventReactor to use for asynchrous events
     * 
     * @param reactor an event reactor
     * @return this Builder
     */
    public B withReactor(EventReactor<ByteBuffer> reactor) {
      this.reactor = reactor;
      return (B) this;
    }

    /**
     * Provide a session identifier
     * 
     * @param sessionId a unique number
     * @return this Builder
     */
    public B withSessionId(UUID sessionId) {
      this.sessionId = sessionId;
      return (B) this;
    }

    /**
     * Provide a non-multiplexed Transport to use for message exchange
     * 
     * @param transport a Transport
     * @return this Builder
     */
    public B withTransport(Transport transport) {
      return (B) withTransport(transport, false);
    }

    /**
     * Provide a Transport to use for message exchange
     * 
     * @param transport a Transport
     * @param isMultiplexed set {@code true} if the transport is multiplexed with other sessions
     * @return this Builder
     */
    public B withTransport(Transport transport, boolean isMultiplexed) {
      this.transport = transport;
      this.isMultiplexedTransport = isMultiplexed;
      return (B) this;
    }
  }

  private class EstablishedHandler implements Receiver {

    @Override
    public void accept(ByteBuffer buffer) {
      setInboundStream();
      setOutboundStream();
      Topic toSendTopic = SessionEventTopics.getTopic(sessionId, APPLICATION_MESSAGE_TO_SEND);
      applicationMessageToSendSubscription = reactor.subscribe(toSendTopic, outboundMessageHandler);
      Topic terminatedTopic = SessionEventTopics.getTopic(sessionId, PEER_TERMINATED);
      terminatedSubscription = reactor.subscribe(terminatedTopic, peerTerminatedHandler);

      sessionSuspendedTopic = SessionEventTopics.getTopic(sessionId, SESSION_SUSPENDED);

      // Notify application that session is ready to go
      Topic readyTopic = SessionEventTopics.getTopic(sessionId, SESSION_READY);
      reactor.post(readyTopic, buffer);
      // System.out.println("FixpSession established");
    }
  }

  @SuppressWarnings("rawtypes")
  public static Builder builder() {
    return new Builder();
  }

  private Subscription applicationMessageToSendSubscription;
  private final Supplier<ByteBuffer> buffers;
  private Subscription establishedSubscription;
  private final Establisher establisher;

  private ExceptionConsumer exceptionConsumer = ex -> {
    System.err.println(ex);
  };

  private FlowReceiver flowReceiver;
  private FlowSender flowSender;
  private FrameSpliterator frameSpliter = new FixpWithMessageLengthFrameSpliterator();
  private boolean isMultiplexedTransport;
  private final MessageConsumer<UUID> messageConsumer;

  private final Receiver negotiatedHandler = new Receiver() {

    @Override
    public void accept(ByteBuffer buffer) {
      // System.out.println("Server negotiated");
      if (negotiatedSubscription != null) {
        negotiatedSubscription.unsubscribe();
      }

      uuidAsBytes = establisher.getSessionId();
      sessionId = SessionId.UUIDFromBytes(uuidAsBytes);

      Topic establishedTopic = SessionEventTopics.getTopic(sessionId, SERVER_ESTABLISHED);
      establishedSubscription = reactor.subscribe(establishedTopic, new EstablishedHandler());
    }
  };

  private Subscription negotiatedSubscription;
  private final FlowType outboundFlow;

  // Supports asynchronous message send
  private final Receiver outboundMessageHandler = new Receiver() {

    @Override
    public void accept(ByteBuffer message) {
      try {
        message.position(message.limit());
        flowSender.send(message);
      } catch (IOException e) {
        exceptionConsumer.accept(e);
      }
    }

  };
  private final Receiver peerTerminatedHandler = new Receiver() {

    @Override
    public void accept(ByteBuffer buffer) {
      close();
      uuidAsBytes = establisher.getSessionId();
      reactor.post(sessionSuspendedTopic, buffer);
      getTransport().close();
    }

  };
  private final EventReactor<ByteBuffer> reactor;
  private UUID sessionId = SessionId.EMPTY;
  private Topic sessionSuspendedTopic;
  private MessageStore store;

  private Subscription terminatedSubscription;

  private final Transport transport;

  private final IdentifiableTransportConsumer<UUID> transportConsumer =
      new IdentifiableTransportConsumer<UUID>() {

        @Override
        public void accept(ByteBuffer buffer) {
          frameSpliter.wrap(buffer);
          frameSpliter.forEachRemaining(flowReceiver);
        }

        @Override
        public void connected() {
          // if client, send Negotiate; otherwise, wait for Negotiate from
          // client
          establisher.connected();
        }

        @Override
        public void disconnected() {
          try {
            flowSender.sendEndOfStream();
          } catch (IOException e) {
            // terminates heartbeats
          }
          reactor.post(sessionSuspendedTopic, null);
        }

        public UUID getSessionId() {
          return FixpSession.this.getSessionId();
        }

      };

  private byte[] uuidAsBytes;

  /**
   * Construct a session
   * 
   * @param builder event pub / sub
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  protected FixpSession(Builder builder) {
    this.reactor = builder.reactor;
    this.transport = builder.transport;
    this.buffers = builder.buffers;
    this.messageConsumer = builder.messageConsumer;
    this.outboundFlow = builder.outboundFlow;

    Objects.requireNonNull(this.reactor);
    Objects.requireNonNull(this.transport);
    Objects.requireNonNull(this.buffers);
    Objects.requireNonNull(this.messageConsumer);
    Objects.requireNonNull(this.outboundFlow);

    this.store = builder.store;
    this.isMultiplexedTransport = builder.isMultiplexedTransport;
    this.sessionId = builder.sessionId;
    this.uuidAsBytes = SessionId.UUIDAsBytes(sessionId);

    if (builder.isServer) {
      this.establisher = createServerEstablisher();
    } else {
      this.establisher = createClientEstablisher(builder.credentials);
    }
    this.flowSender = (FlowSender) this.establisher;
    this.flowReceiver = (FlowReceiver) this.establisher;

    this.establisher.withOutboundKeepaliveInterval(builder.outboundKeepaliveInterval);

    if (builder.frameSpliter != null) {
      this.frameSpliter = builder.frameSpliter;
    }
    if (builder.exceptionHandler != null) {
      this.exceptionConsumer = builder.exceptionHandler;
    }
  }

  /**
   * Terminate this FixpSession
   */
  @Override
  public void close() {
    try {
      flowSender.sendEndOfStream();
    } catch (IOException e) {
      getTransport().close();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof FixpSession)) {
      return false;
    }
    FixpSession other = (FixpSession) obj;
    if (sessionId == null || sessionId.equals(SessionId.EMPTY) || other.sessionId == null
        || other.sessionId.equals(SessionId.EMPTY)) {
      return super.equals(obj);
    } else if (!sessionId.equals(other.sessionId)) {
      return false;
    }
    return true;
  }

  public long getNextSeqNoToReceive() {
    if (flowReceiver instanceof Sequenced) {
      Sequenced sequenced = (Sequenced) flowReceiver;
      return sequenced.getNextSeqNo();
    } else {
      return 0;
    }
  }

  /**
   * Returns the outbound FlowType
   * 
   * @return outbound flow
   */
  public FlowType getOutboundFlow() {
    return outboundFlow;
  }

  /**
   * @return the sessionId
   */
  public UUID getSessionId() {
    return sessionId;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    if (!sessionId.equals(SessionId.EMPTY)) {
      final int prime = 31;
      int result = 1;
      result = prime * result + sessionId.hashCode();
      return result;
    } else {
      return super.hashCode();
    }
  }

  /**
   * Tests whether this Session establishes in a server role
   * 
   * @return Returns {@code true} if this is a server Session
   */
  public boolean isServer() {
    return establisher instanceof ServerSessionEstablisher;
  }

  @Override
  public void open() throws IOException {
    getTransport().open(getBuffers(), getTransportConsumer());
  }

  public void resend(ByteBuffer message, long seqNo, long requestTimestamp) throws IOException {
    if (flowSender instanceof RecoverableSender) {
      ((RecoverableSender) flowSender).resend(message, seqNo, requestTimestamp);
    }
  }

  public void resend(ByteBuffer[] messages, int offset, int length, long seqNo,
      long requestTimestamp) throws IOException {
    if (flowSender instanceof RecoverableSender) {
      ((RecoverableSender) flowSender).resend(messages, offset, length, seqNo, requestTimestamp);
    }
  }

  public long send(ByteBuffer message) throws IOException {
    return flowSender.send(message);
  }

  public long send(ByteBuffer[] messages) throws IOException {
    return flowSender.send(messages);
  }

  public void setNextSeqNoToSend(long nextSeqNo) {
    if (flowSender instanceof MutableSequence) {
      MutableSequence sequenced = (MutableSequence) flowSender;
      sequenced.setNextSeqNo(nextSeqNo);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("FixpSession [");
    if (sessionId != null) {
      builder.append("sessionId=");
      builder.append(sessionId);
      builder.append(", ");
    }
    if (getTransport() != null) {
      builder.append("getTransport()=");
      builder.append(getTransport());
    }
    builder.append("]");
    return builder.toString();
  }

  private Establisher createClientEstablisher(byte[] credentials) {
    final ClientSessionEstablisher clientSessionEstablisher =
        new ClientSessionEstablisher(reactor, outboundFlow, getTransport()).withCredentials(
            sessionId, credentials);

    Topic initTopic = SessionEventTopics.getTopic(sessionId, CLIENT_ESTABLISHED);
    establishedSubscription = reactor.subscribe(initTopic, new EstablishedHandler());

    return clientSessionEstablisher;
  }

  private Establisher createServerEstablisher() {
    final ServerSessionEstablisher serverSessionEstablisher =
        new ServerSessionEstablisher(reactor, getTransport(), outboundFlow);

    // Since subscription occurs before sessionId is available, use
    // transport hashCode
    // as unique identifier.
    Topic initTopic = SessionEventTopics.getTopic(SERVER_NEGOTIATED, getTransport().hashCode());
    negotiatedSubscription = reactor.subscribe(initTopic, negotiatedHandler);

    return serverSessionEstablisher;
  }

  private void setInboundStream() {
    switch (establisher.getInboundFlow()) {
      case UNSEQUENCED:
        this.flowReceiver =
            new UnsequencedFlowReceiver(reactor, this, messageConsumer,
                establisher.getInboundKeepaliveInterval());
        break;
      case IDEMPOTENT:
        this.flowReceiver =
            new IdempotentFlowReceiver(reactor, this, messageConsumer,
                establisher.getInboundKeepaliveInterval());
        break;
      case RECOVERABLE:
        this.flowReceiver =
            new RecoverableFlowReceiver(reactor, this, messageConsumer,
                establisher.getInboundKeepaliveInterval());
        break;
      case NONE:
        this.flowReceiver =
            new NoneReceiver(reactor, sessionId, establisher.getInboundKeepaliveInterval());
        break;
    }
  }

  private void setOutboundStream() {
    switch (establisher.getOutboundFlow()) {
      case UNSEQUENCED:
        this.flowSender =
            new UnsequencedFlowSender(reactor, sessionId, getTransport(),
                establisher.getOutboundKeepaliveInterval());
        break;
      case IDEMPOTENT:
        Sequencer sequencer =
            isMultiplexedTransport ? new MultiplexSequencer(uuidAsBytes)
                : (getTransport().isFifo() ? new SimplexStreamSequencer() : new SimplexSequencer());

        this.flowSender =
            new IdempotentFlowSender(reactor, sessionId, getTransport(),
                establisher.getOutboundKeepaliveInterval(), sequencer);
        break;
      case RECOVERABLE:
        this.flowSender =
            new RecoverableFlowSender(reactor, store, sessionId, getTransport(),
                establisher.getOutboundKeepaliveInterval(),
                isMultiplexedTransport ? new MultiplexSequencer(uuidAsBytes)
                    : new SimplexSequencer());
        break;
      case NONE:
        this.flowSender =
            new NoneSender(reactor, sessionId, getTransport(),
                establisher.getOutboundKeepaliveInterval());
        break;
    }
  }

  protected Supplier<ByteBuffer> getBuffers() {
    return buffers;
  }

  /**
   * Returns the Transport to communicate with peer
   * 
   * @return a message Transport
   */
  protected Transport getTransport() {
    return transport;
  }

  protected IdentifiableTransportConsumer<UUID> getTransportConsumer() {
    return this.transportConsumer;
  }
}
