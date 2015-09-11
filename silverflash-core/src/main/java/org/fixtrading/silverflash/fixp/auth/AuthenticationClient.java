package org.fixtrading.silverflash.fixp.auth;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.SERVICE_AUTHENTICATE;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.ToSessionEventType.*;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.Topic;

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
