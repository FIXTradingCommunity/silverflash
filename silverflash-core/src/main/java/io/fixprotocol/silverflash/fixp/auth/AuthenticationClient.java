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

package io.fixprotocol.silverflash.fixp.auth;

import static io.fixprotocol.silverflash.fixp.SessionEventTopics.ServiceEventType.SERVICE_AUTHENTICATE;
import static io.fixprotocol.silverflash.fixp.SessionEventTopics.ToSessionEventType.*;

import java.nio.ByteBuffer;
import java.util.UUID;

import io.fixprotocol.silverflash.Receiver;
import io.fixprotocol.silverflash.fixp.SessionEventTopics;
import io.fixprotocol.silverflash.reactor.EventReactor;
import io.fixprotocol.silverflash.reactor.Subscription;
import io.fixprotocol.silverflash.reactor.Topic;

/**
 * Makes requests to an Authenticator with asynchronous response
 * 
 * @author Don Mendelson
 *
 */
public class AuthenticationClient {

  private final Receiver authenticatedHandler = new Receiver() {
    @Override
    public void accept(ByteBuffer buffer) {
      listener.authenticated(sessionId);
    }
  };

  private Subscription authenticatedSubscription;

  private AuthenticationListener listener;
  private final Receiver notAuthenticatedHandler = new Receiver() {
    @Override
    public void accept(ByteBuffer buffer) {
      listener.authenticationFailed(sessionId);
    }
  };
  private Subscription notAuthenticatedSubscription;
  private final EventReactor<ByteBuffer> reactor;

  private UUID sessionId;

  /**
   * Constructor
   * 
   * @param reactor event pub/sub
   */
  public AuthenticationClient(EventReactor<ByteBuffer> reactor) {
    this.reactor = reactor;
  }

  /**
   * Make an asynchronous request to an Authenticator
   * 
   * @param sessionId session to be authenticated
   * @param negotiation message containing credentials
   * @param listener a listener for results
   */
  public void requestAuthentication(UUID sessionId, final ByteBuffer negotiation,
      AuthenticationListener listener) {
    this.sessionId = sessionId;
    this.listener = listener;
    Topic authenticatedTopic = SessionEventTopics.getTopic(sessionId, AUTHENTICATED);
    authenticatedSubscription = reactor.subscribe(authenticatedTopic, authenticatedHandler);

    Topic notAuthenticatedTopic = SessionEventTopics.getTopic(sessionId, NOT_AUTHENTICATED);
    notAuthenticatedSubscription =
        reactor.subscribe(notAuthenticatedTopic, notAuthenticatedHandler);

    // Authenticate (async)
    Topic authTopic = SessionEventTopics.getTopic(SERVICE_AUTHENTICATE);
    reactor.post(authTopic, negotiation);
  }
}
