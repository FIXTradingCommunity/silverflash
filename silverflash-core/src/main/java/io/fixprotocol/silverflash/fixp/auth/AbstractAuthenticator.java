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

package io.fixprotocol.silverflash.fixp.auth;

import static io.fixprotocol.silverflash.fixp.SessionEventTopics.ServiceEventType.SERVICE_AUTHENTICATE;
import static io.fixprotocol.silverflash.fixp.SessionEventTopics.ToSessionEventType.AUTHENTICATED;
import static io.fixprotocol.silverflash.fixp.SessionEventTopics.ToSessionEventType.NOT_AUTHENTICATED;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import io.fixprotocol.silverflash.Receiver;
import io.fixprotocol.silverflash.fixp.SessionEventTopics;
import io.fixprotocol.silverflash.fixp.SessionId;
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderDecoder;
import io.fixprotocol.silverflash.fixp.messages.NegotiateDecoder;
import io.fixprotocol.silverflash.reactor.EventReactor;
import io.fixprotocol.silverflash.reactor.Subscription;
import io.fixprotocol.silverflash.reactor.Topic;

/**
 * A base class for authentication services
 * 
 * @author Don Mendelson
 *
 */
abstract class AbstractAuthenticator implements ReactiveAuthenticator<UUID, ByteBuffer> {

  private final Receiver authenticateHandler = new Receiver() {

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final NegotiateDecoder negotiateDecoder = new NegotiateDecoder();
    private final DirectBuffer directBuffer = new UnsafeBuffer(new byte[0]);

    @Override
    public void accept(ByteBuffer buffer) {
      directBuffer.wrap(buffer);
      messageHeaderDecoder.wrap(directBuffer, buffer.position());
      if (messageHeaderDecoder.templateId() == negotiateDecoder.sbeTemplateId() &&
              messageHeaderDecoder.schemaId() == negotiateDecoder.sbeSchemaId()) {
        negotiateDecoder.wrap(directBuffer, messageHeaderDecoder.encodedLength(), negotiateDecoder.sbeBlockLength(),
                negotiateDecoder.sbeSchemaVersion());

          byte[] credentials = new byte[128];
          negotiateDecoder.getCredentials(credentials, 0, credentials.length);
          byte[] sessionId = new byte[16];
          for (int i=0; i<16; i++) {
            sessionId[i] = (byte)negotiateDecoder.sessionId(i);
          }
          UUID uuid = SessionId.UUIDFromBytes(sessionId);
          doAuthenticate(buffer, credentials, sessionId, uuid);
        } else {
          System.err.println("Authenticator: unexpected message type received");
        }
      }
    };

  private Topic authenticateTopic;
  private EventReactor<ByteBuffer> reactor;
  private Subscription serviceAuthenticateSubscription;

  public void close() {
    reactor.unsubscribe(authenticateTopic);
  }

  public CompletableFuture<? extends AbstractAuthenticator> open() {
    authenticateTopic = SessionEventTopics.getTopic(SERVICE_AUTHENTICATE);
    serviceAuthenticateSubscription = reactor.subscribe(authenticateTopic, authenticateHandler);
    if (serviceAuthenticateSubscription == null) {
      CompletableFuture<AbstractAuthenticator> future = new CompletableFuture<>();
      future.completeExceptionally(
          new RuntimeException("Failed to open Authenticator; subscription failed for topic "
              + authenticateTopic.toString()));
      return future;
    } else {
      return CompletableFuture.completedFuture(this);
    }
  }

  public ReactiveAuthenticator<UUID, ByteBuffer> withEventReactor(
      EventReactor<ByteBuffer> reactor) {
    Objects.requireNonNull(reactor);
    this.reactor = reactor;
    return this;
  }

  protected void doAuthenticate(ByteBuffer buffer, byte[] credentials, byte[] sessionId,
      UUID uuid) {
    if (authenticate(uuid, credentials)) {
      onAuthenticated(SessionId.UUIDFromBytes(sessionId), buffer);
    } else {
      onAuthenticationFailed(SessionId.UUIDFromBytes(sessionId), buffer);
    }
  }

  /**
   * Returns the EventReactor used by this Authenticator
   * 
   * @return the reactor
   */
  protected EventReactor<ByteBuffer> getReactor() {
    return reactor;
  }

  protected void onAuthenticated(UUID sessionId, ByteBuffer buffer) {
    Topic authenticatedTopic = SessionEventTopics.getTopic(sessionId, AUTHENTICATED);
    buffer.rewind();
    reactor.post(authenticatedTopic, buffer);
  }

  protected void onAuthenticationFailed(UUID sessionId, ByteBuffer buffer) {
    Topic notAuthenticatedTopic = SessionEventTopics.getTopic(sessionId, NOT_AUTHENTICATED);
    buffer.rewind();
    reactor.post(notAuthenticatedTopic, buffer);

  }
}
