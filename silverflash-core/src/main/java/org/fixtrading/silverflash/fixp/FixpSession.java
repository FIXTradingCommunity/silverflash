/**
 * Copyright 2015-2016 FIX Protocol Ltd
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

import static org.fixtrading.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_READY;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_SUSPENDED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.CLIENT_ESTABLISHED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.MULTICAST_TOPIC;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.PEER_TERMINATED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.SERVER_ESTABLISHED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.SERVER_NEGOTIATED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.ToSessionEventType.APPLICATION_MESSAGE_TO_SEND;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.fixtrading.silverflash.ExceptionConsumer;
import org.fixtrading.silverflash.MessageConsumer;
import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.RecoverableSender;
import org.fixtrading.silverflash.Sequenced;
import org.fixtrading.silverflash.Session;
import org.fixtrading.silverflash.buffer.BufferSupplier;
import org.fixtrading.silverflash.fixp.flow.ClientSessionEstablisher;
import org.fixtrading.silverflash.fixp.flow.FlowBuilder;
import org.fixtrading.silverflash.fixp.flow.FlowReceiver;
import org.fixtrading.silverflash.fixp.flow.FlowReceiverBuilder;
import org.fixtrading.silverflash.fixp.flow.FlowSender;
import org.fixtrading.silverflash.fixp.flow.IdempotentFlowReceiver;
import org.fixtrading.silverflash.fixp.flow.IdempotentFlowSender;
import org.fixtrading.silverflash.fixp.flow.IdempotentFlowSenderWithTopic;
import org.fixtrading.silverflash.fixp.flow.MulticastConsumerEstablisher;
import org.fixtrading.silverflash.fixp.flow.MulticastProducerEstablisher;
import org.fixtrading.silverflash.fixp.flow.MultiplexSequencer;
import org.fixtrading.silverflash.fixp.flow.MutableSequence;
import org.fixtrading.silverflash.fixp.flow.NoneFlowReceiver;
import org.fixtrading.silverflash.fixp.flow.NoneFlowSender;
import org.fixtrading.silverflash.fixp.flow.RecoverableFlowReceiver;
import org.fixtrading.silverflash.fixp.flow.RecoverableFlowSender;
import org.fixtrading.silverflash.fixp.flow.Sequencer;
import org.fixtrading.silverflash.fixp.flow.ServerSessionEstablisher;
import org.fixtrading.silverflash.fixp.flow.SimplexSequencer;
import org.fixtrading.silverflash.fixp.flow.SimplexStreamSequencer;
import org.fixtrading.silverflash.fixp.flow.UnsequencedFlowReceiver;
import org.fixtrading.silverflash.fixp.flow.UnsequencedFlowSender;
import org.fixtrading.silverflash.fixp.messages.FlowType;
import org.fixtrading.silverflash.fixp.store.MessageStore;
import org.fixtrading.silverflash.frame.FrameSpliterator;
import org.fixtrading.silverflash.frame.MessageFrameEncoder;
import org.fixtrading.silverflash.frame.MessageLengthFrameEncoder;
import org.fixtrading.silverflash.frame.MessageLengthFrameSpliterator;
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

    private BufferSupplier buffers;
    private byte[] credentials = null;
    private ExceptionConsumer exceptionHandler;
    private FrameSpliterator frameSpliter = null;
    private boolean isMultiplexedTransport = false;
    private MessageConsumer<UUID> messageConsumer = null;
    private FlowType outboundFlow = FlowType.Idempotent;
    private int outboundKeepaliveInterval = 10000;
    private EventReactor<ByteBuffer> reactor = null;
    private Role role = Role.CLIENT;
    private UUID sessionId = SessionId.EMPTY;
    private MessageStore store = null;
    private String topic;
    private Transport transport = null;
    private MessageFrameEncoder frameEncoder;

    /**
     * This session will play the client role in a point-to-point session (default)
     * 
     * @return this Builder
     */
    public B asClient() {
      this.role = Role.CLIENT;
      return (B) this;
    }

    /**
     * This session will play the consumer role in a multicast session
     * 
     * @return this Builder
     */
    public B asMulticastConsumer() {
      this.role = Role.MULTICAST_CONSUMER;
      return (B) this;
    }

    /**
     * This session will play the publisher role in a multicast session
     * 
     * @return this Builder
     */
    public B asMulticastPublisher() {
      this.role = Role.MULTICAST_PRODUCER;
      return (B) this;
    }

    /**
     * This session will play the server role in a point-to-point session
     * 
     * @return this Builder
     */
    public B asServer() {
      this.role = Role.SERVER;
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
    public B withBufferSupplier(BufferSupplier buffers) {
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
     * Provide a message framer for received messages. If not provided, a default implementation is
     * used.
     * 
     * @param frameSpliter the FrameSpliterator to use for received messages
     * @return this Builder
     */
    public B withMessageFramer(FrameSpliterator frameSpliter) {
      this.frameSpliter = frameSpliter;
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
     * Provide category of application messages for multicast
     * 
     * @param topic category of application messages
     * @return this Builder
     */
    public B withTopic(String topic) {
      this.topic = topic;
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

  private enum Role {
    CLIENT, MULTICAST_CONSUMER, MULTICAST_PRODUCER, SERVER
  }

  @SuppressWarnings("rawtypes")
  public static Builder builder() {
    return new Builder();
  }

  private Subscription applicationMessageToSendSubscription;
  private final BufferSupplier buffers;

  private final Receiver establishedHandler = new Receiver() {

    @Override
    public void accept(ByteBuffer buffer) {
      setInboundStream();
      setOutboundStream();
      Topic toSendTopic = SessionEventTopics.getTopic(sessionId, APPLICATION_MESSAGE_TO_SEND);
      applicationMessageToSendSubscription = reactor.subscribe(toSendTopic, outboundMessageHandler);
      Topic terminatedTopic = SessionEventTopics.getTopic(sessionId, PEER_TERMINATED);
      terminatedSubscription = reactor.subscribe(terminatedTopic, peerTerminatedHandler);

      sessionSuspendedTopic = SessionEventTopics.getTopic(sessionId, SESSION_SUSPENDED);

      try {
        establisher.complete();

        // Notify application that session is ready to go
        Topic readyTopic = SessionEventTopics.getTopic(sessionId, SESSION_READY);
        reactor.post(readyTopic, buffer);
        // System.out.println("FixpSession established");
      } catch (IOException e) {
        exceptionConsumer.accept(e);
      }
    }
  };

  private Subscription establishedSubscription;
  private final Establisher establisher;
  private ExceptionConsumer exceptionConsumer = System.err::println;
  private FlowReceiver flowReceiver;
  private FlowSender flowSender;
  private final FrameSpliterator frameSpliter;
  private final boolean isMultiplexedTransport;
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
      establishedSubscription = reactor.subscribe(establishedTopic, establishedHandler);
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
  private final Role role;
  private UUID sessionId = SessionId.EMPTY;
  private Topic sessionSuspendedTopic;
  private final MessageStore store;
  private Subscription terminatedSubscription;

  private final Receiver topicHandler = new Receiver() {

    @Override
    public void accept(ByteBuffer buffer) {
      if (establishedSubscription != null) {
        establishedSubscription.unsubscribe();
      }

      uuidAsBytes = establisher.getSessionId();
      sessionId = SessionId.UUIDFromBytes(uuidAsBytes);

      setInboundStream();
      setOutboundStream();

      try {
        establisher.complete();
      } catch (IOException e) {
        exceptionConsumer.accept(e);
      }
    }
  };

  private final Transport transport;

  private final IdentifiableTransportConsumer<UUID> transportConsumer =
      new IdentifiableTransportConsumer<UUID>() {

        @Override
        public void accept(ByteBuffer buffer) {
          if (getTransport().isMessageOriented()) {
            flowReceiver.accept(buffer);
          } else {
            frameSpliter.wrap(buffer);
            frameSpliter.forEachRemaining(flowReceiver);
          }
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
  private final String topic;
  private MessageFrameEncoder frameEncoder;

  /**
   * Construct a session
   * 
   * @param builder event pub / sub
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  protected FixpSession(Builder builder) {
    Objects.requireNonNull(builder.reactor);
    Objects.requireNonNull(builder.transport);

    this.reactor = builder.reactor;
    this.transport = builder.transport;
    this.buffers = builder.buffers;
    this.messageConsumer = builder.messageConsumer;
    this.outboundFlow = builder.outboundFlow;

    this.store = builder.store;
    this.isMultiplexedTransport = builder.isMultiplexedTransport;
    this.sessionId = builder.sessionId;
    this.uuidAsBytes = SessionId.UUIDAsBytes(sessionId);

    if (builder.frameSpliter != null) {
      this.frameSpliter = builder.frameSpliter;
    } else {
      this.frameSpliter = new MessageLengthFrameSpliterator();
    }

    if (builder.frameEncoder != null) {
      this.frameEncoder = builder.frameEncoder;
    } else {
      this.frameEncoder = new MessageLengthFrameEncoder();
    }
    
    this.role = builder.role;
    this.topic = builder.topic;

    switch (role) {
      case SERVER:
        Objects.requireNonNull(this.messageConsumer);
        Objects.requireNonNull(this.buffers);
        Objects.requireNonNull(this.outboundFlow);
        this.establisher = createServerEstablisher();
        break;
      case CLIENT:
        Objects.requireNonNull(this.messageConsumer);
        Objects.requireNonNull(this.buffers);
        Objects.requireNonNull(this.outboundFlow);
        this.establisher = createClientEstablisher(builder.credentials);
        break;
      case MULTICAST_PRODUCER:
        Objects.requireNonNull(this.outboundFlow);
        this.establisher = createMulticastProducerEstablisher(topic);
        break;
      case MULTICAST_CONSUMER:
        Objects.requireNonNull(this.messageConsumer);
        Objects.requireNonNull(this.buffers);
        this.establisher = createMulticastConsumerEstablisher(topic);
        break;
      default:
        this.establisher = null;
    }

    this.flowSender = (FlowSender) this.establisher;
    this.flowReceiver = (FlowReceiver) this.establisher;

    this.establisher.withOutboundKeepaliveInterval(builder.outboundKeepaliveInterval);


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

  private Establisher createClientEstablisher(byte[] credentials) {
    final ClientSessionEstablisher clientSessionEstablisher =
        new ClientSessionEstablisher(frameEncoder.copy(), reactor, outboundFlow, getTransport())
            .withCredentials(sessionId, credentials);

    Topic initTopic = SessionEventTopics.getTopic(sessionId, CLIENT_ESTABLISHED);
    establishedSubscription = reactor.subscribe(initTopic, establishedHandler);

    return clientSessionEstablisher;
  }

  private Establisher createMulticastConsumerEstablisher(String topic) {
    final MulticastConsumerEstablisher clientSessionEstablisher =
        new MulticastConsumerEstablisher(reactor, getTransport()).withTopic(topic);

    Topic initTopic = SessionEventTopics.getTopic(MULTICAST_TOPIC, topic);
    establishedSubscription = reactor.subscribe(initTopic, topicHandler);

    return clientSessionEstablisher;
  }

  private Establisher createMulticastProducerEstablisher(String topic) {
    final MulticastProducerEstablisher serverSessionEstablisher = new MulticastProducerEstablisher(
        frameEncoder.copy(), reactor, getTransport(), outboundFlow, topic, sessionId);

    Topic initTopic = SessionEventTopics.getTopic(MULTICAST_TOPIC, topic);
    establishedSubscription = reactor.subscribe(initTopic, establishedHandler);

    return serverSessionEstablisher;
  }

  private Establisher createServerEstablisher() {
    final ServerSessionEstablisher serverSessionEstablisher =
        new ServerSessionEstablisher(frameEncoder.copy(), reactor, getTransport(), outboundFlow);

    // Since subscription occurs before sessionId is available, use
    // transport hashCode
    // as unique identifier.
    Topic initTopic = SessionEventTopics.getTopic(SERVER_NEGOTIATED, getTransport().hashCode());
    negotiatedSubscription = reactor.subscribe(initTopic, negotiatedHandler);

    return serverSessionEstablisher;
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

  protected BufferSupplier getBuffers() {
    return buffers;
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

  /**
   * Returns the Transport to communicate with peer
   * 
   * @return a message Transport
   */
  protected Transport getTransport() {
    return transport;
  }

  public IdentifiableTransportConsumer<UUID> getTransportConsumer() {
    return this.transportConsumer;
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
  public CompletableFuture<FixpSession> open() {
    CompletableFuture<FixpSession> future = new CompletableFuture<>();
    getTransport().open(getBuffers(), getTransportConsumer()).whenComplete((transport, error) -> {
      if (error == null) {
        future.complete(this);
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
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

  private void setInboundStream() {
    @SuppressWarnings("rawtypes")
    FlowReceiverBuilder<? extends FlowReceiver, ? extends FlowReceiverBuilder> builder = null;
    switch (establisher.getInboundFlow()) {
      case Unsequenced:
        builder = UnsequencedFlowReceiver.builder();
        break;
      case Idempotent:
        builder = IdempotentFlowReceiver.builder();
        break;
      case Recoverable:
        builder = RecoverableFlowReceiver.builder();
        break;
      case None:
        builder = NoneFlowReceiver.builder();
        break;
    }
    this.flowReceiver =
        (FlowReceiver) builder.withSession(this).withMessageConsumer(messageConsumer)
            .withReactor(reactor).withTransport(getTransport())
            .withKeepaliveInterval(establisher.getInboundKeepaliveInterval())
            .withMessageFrameEncoder(frameEncoder.copy())
            .withExceptionConsumer(exceptionConsumer).build();
  }

  public void setNextSeqNoToSend(long nextSeqNo) {
    if (flowSender instanceof MutableSequence) {
      MutableSequence sequenced = (MutableSequence) flowSender;
      sequenced.setNextSeqNo(nextSeqNo);
    }
  }

  private void setOutboundStream() {
    @SuppressWarnings("rawtypes")
    FlowBuilder builder = null;
    Sequencer sequencer;
    switch (establisher.getOutboundFlow()) {
      case Unsequenced:
        builder = UnsequencedFlowSender.builder();
        break;
      case Idempotent:
        if (Role.MULTICAST_PRODUCER == role) {
          @SuppressWarnings("rawtypes")
          IdempotentFlowSenderWithTopic.Builder aBuilder = IdempotentFlowSenderWithTopic.builder();
          aBuilder.withTopic(topic);
          builder = aBuilder;
        } else {
          builder = IdempotentFlowSender.builder();
        }

        sequencer = isMultiplexedTransport ? new MultiplexSequencer(frameEncoder.copy(), uuidAsBytes)
            : (getTransport().isFifo() ? new SimplexStreamSequencer(frameEncoder.copy())
                : new SimplexSequencer(frameEncoder));

        builder.withSequencer(sequencer);
        break;
      case Recoverable:
        @SuppressWarnings("rawtypes")
        RecoverableFlowSender.Builder abuilder = RecoverableFlowSender.builder();
        sequencer = isMultiplexedTransport ? new MultiplexSequencer(frameEncoder.copy(), uuidAsBytes)
            : (getTransport().isFifo() ? new SimplexStreamSequencer(frameEncoder.copy())
                : new SimplexSequencer(frameEncoder));

        abuilder.withMessageStore(store).withSequencer(sequencer);
        builder = abuilder;
        break;
      case None:
        builder = NoneFlowSender.builder();
        break;
    }

    this.flowSender =
        (FlowSender) builder.withKeepaliveInterval(establisher.getOutboundKeepaliveInterval())
            .withReactor(reactor).withSessionId(getSessionId()).withTransport(getTransport())
            .withMessageFrameEncoder(frameEncoder.copy())
            .withExceptionConsumer(exceptionConsumer).build();
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
}
