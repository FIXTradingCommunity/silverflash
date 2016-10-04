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
package io.fixprotocol.silverflash.examples;

import java.nio.ByteBuffer;
import java.util.UUID;

import io.fixprotocol.silverflash.MessageConsumer;
import io.fixprotocol.silverflash.buffer.BufferSupplier;
import io.fixprotocol.silverflash.fixp.FixpSession;
import io.fixprotocol.silverflash.fixp.Sessions;
import io.fixprotocol.silverflash.fixp.messages.FlowType;
import io.fixprotocol.silverflash.fixp.store.MessageStore;
import io.fixprotocol.silverflash.frame.MessageLengthFrameEncoder;
import io.fixprotocol.silverflash.reactor.EventReactor;
import io.fixprotocol.silverflash.transport.Transport;

/**
 * Creates new instances of FixpSession
 * 
 * @author Don Mendelson
 *
 */
public class FixpSessionFactory extends Sessions {

  private final int outboundKeepaliveInterval;
  private final EventReactor<ByteBuffer> reactor;
  private MessageStore store;
  private final boolean isMultiplexed;

  /**
   * Construct a factory without a MessageStore. No recovery is supported and not multiplexed.
   * 
   * @param reactor event pub/sub
   * @param outboundKeepaliveInterval heartbeat interval for created sessions
   */
  public FixpSessionFactory(EventReactor<ByteBuffer> reactor, int outboundKeepaliveInterval) {
    this(reactor, outboundKeepaliveInterval, false);
  }

  /**
   * Construct a factory without a MessageStore. No recovery is supported.
   * 
   * @param reactor event pub/sub
   * @param outboundKeepaliveInterval heartbeat interval for created sessions
   * @param isMultiplexed sessions share a transport
   */
  public FixpSessionFactory(EventReactor<ByteBuffer> reactor, int outboundKeepaliveInterval,
      boolean isMultiplexed) {
    this.reactor = reactor;
    this.outboundKeepaliveInterval = outboundKeepaliveInterval;
    this.isMultiplexed = isMultiplexed;
  }

  /**
   * Construct a factory without a MessageStore. Recovery is supported.
   * 
   * @param reactor event pub/sub
   * @param outboundKeepaliveInterval heartbeat interval for created sessions
   * @param store message repository
   */
  public FixpSessionFactory(EventReactor<ByteBuffer> reactor, int outboundKeepaliveInterval,
      boolean isMultiplexed, MessageStore store) {
    this(reactor, outboundKeepaliveInterval, isMultiplexed);
    this.store = store;
  }

  /**
   * Creates a new client session
   * 
   * @param credentials session identification for authentication
   * @param transport a communication channel to be opened by the session
   * @param buffers a buffer Supplier for received messages
   * @param streamReceiver application layer message consumer
   * @param outboundFlow type of the outbound message flow
   * @return a new FixpSession
   */
  public FixpSession createClientSession(byte[] credentials, Transport transport,
      BufferSupplier buffers, MessageConsumer<UUID> streamReceiver, FlowType outboundFlow) {
    FixpSession session;
    if (outboundFlow == FlowType.Recoverable) {
      session =
          FixpSession.builder().withReactor(reactor).withTransport(transport, isMultiplexed)
              .withBufferSupplier(buffers).withMessageConsumer(streamReceiver)
              .withOutboundFlow(outboundFlow).withSessionId(UUID.randomUUID())
              .withClientCredentials(credentials)
              .withMessageFrameEncoder(new MessageLengthFrameEncoder())
              .withOutboundKeepaliveInterval(outboundKeepaliveInterval).withMessageStore(store)
              .build();
    } else {
      session =
          FixpSession.builder().withReactor(reactor).withTransport(transport, isMultiplexed)
              .withBufferSupplier(buffers).withMessageConsumer(streamReceiver)
              .withOutboundFlow(outboundFlow).withSessionId(UUID.randomUUID())
              .withClientCredentials(credentials)
              .withOutboundKeepaliveInterval(outboundKeepaliveInterval).build();
    }

    addSession(session);
    return session;
  }

  /**
   * Creates a new server session
   * 
   * @param transport a communication channel to be opened by the session
   * @param buffers a buffer Supplier for received messages
   * @param streamReceiver application layer message consumer
   * @param outboundFlow type of the outbound message flow
   * @return a new FixpSession
   */
  public FixpSession createServerSession(Transport transport, BufferSupplier buffers,
      MessageConsumer<UUID> streamReceiver, FlowType outboundFlow) {
    FixpSession session;
    if (outboundFlow == FlowType.Recoverable) {
      session =
          FixpSession.builder().withReactor(reactor).withTransport(transport, isMultiplexed)
              .withBufferSupplier(buffers).withMessageConsumer(streamReceiver)
              .withOutboundFlow(outboundFlow)
              .withMessageFrameEncoder(new MessageLengthFrameEncoder())
              .withOutboundKeepaliveInterval(outboundKeepaliveInterval).withMessageStore(store)
              .asServer().build();
    } else {
      session =
          FixpSession.builder().withReactor(reactor).withTransport(transport, isMultiplexed)
              .withBufferSupplier(buffers).withMessageConsumer(streamReceiver)
              .withOutboundFlow(outboundFlow)
              .withMessageFrameEncoder(new MessageLengthFrameEncoder())
              .withOutboundKeepaliveInterval(outboundKeepaliveInterval).asServer().build();
    }

    // Can't add to session to map yet because don't session ID until
    // negotiated
    addNewSession(session);
    return session;
  }

}
