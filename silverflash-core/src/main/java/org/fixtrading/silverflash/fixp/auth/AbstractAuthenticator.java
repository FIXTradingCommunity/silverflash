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

package org.fixtrading.silverflash.fixp.auth;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.SERVICE_AUTHENTICATE;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.ToSessionEventType.AUTHENTICATED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.ToSessionEventType.NOT_AUTHENTICATED;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageType;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.Decoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.NegotiateDecoder;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.Topic;

/**
 * A base class for authentication services
 * 
 * @author Don Mendelson
 *
 */
abstract class AbstractAuthenticator implements ReactiveAuthenticator<UUID, ByteBuffer> {

  private final Receiver authenticateHandler = new Receiver() {

    @Override
    public void accept(ByteBuffer buffer) {
      Optional<Decoder> optDecoder = messageDecoder.attachForDecode(buffer, buffer.position());
      if (optDecoder.isPresent()) {
        final Decoder decoder = optDecoder.get();
        if (decoder.getMessageType() == MessageType.NEGOTIATE) {
          NegotiateDecoder negotiateDecoder = (NegotiateDecoder) decoder;
          byte[] credentials = new byte[128];
          negotiateDecoder.getCredentials(credentials, 0);
          byte[] sessionId = new byte[16];
          negotiateDecoder.getSessionId(sessionId, 0);
          UUID uuid = SessionId.UUIDFromBytes(sessionId);
          doAuthenticate(buffer, credentials, sessionId, uuid);
        } else {
          System.err.println("Authenticator: unexpected message type received");
        }
      }
    }
  };

  private Topic authenticateTopic;
  private final MessageDecoder messageDecoder = new MessageDecoder();
  private EventReactor<ByteBuffer> reactor;
  private Subscription serviceAuthenticateSubscription;

  public void close() {
    reactor.unsubscribe(authenticateTopic);
    System.out.println("Authenticator closed");
  }

  public CompletableFuture<? extends AbstractAuthenticator> open() {
    authenticateTopic = SessionEventTopics.getTopic(SERVICE_AUTHENTICATE);
    serviceAuthenticateSubscription = reactor.subscribe(authenticateTopic, authenticateHandler);
    if (serviceAuthenticateSubscription != null) {
      System.out.println("Authenticator open");
    } else {
      CompletableFuture<AbstractAuthenticator> future = new CompletableFuture<>();
      future.completeExceptionally(new RuntimeException(
          "Failed to open Authenticator; subscription failed for topic "
              + authenticateTopic.toString()));
      return future;
    }
    return CompletableFuture.completedFuture(this);
  }

  public ReactiveAuthenticator<UUID, ByteBuffer> withEventReactor(EventReactor<ByteBuffer> reactor) {
    Objects.requireNonNull(reactor);
    this.reactor = reactor;
    return this;
  }

  protected void doAuthenticate(ByteBuffer buffer, byte[] credentials, byte[] sessionId, UUID uuid) {
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
