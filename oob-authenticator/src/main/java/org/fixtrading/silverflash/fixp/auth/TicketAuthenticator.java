package org.fixtrading.silverflash.fixp.auth;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.SERVICE_CREDENTIALS_RETRIEVED;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.auth.Credentials;
import org.fixtrading.silverflash.auth.Entity;
import org.fixtrading.silverflash.auth.Ticket;
import org.fixtrading.silverflash.auth.TicketCallback;
import org.fixtrading.silverflash.auth.TicketsCallback;
import org.fixtrading.silverflash.auth.proxy.CredentialsResponseMessage;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.auth.AbstractAuthenticator;
import org.fixtrading.silverflash.fixp.auth.SessionIdCallback;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.Topic;

/**
 * 
 * 
 * @author Don Mendelson
 *
 */
public class TicketAuthenticator extends AbstractAuthenticator {

  private static final String CONFIG_NAME = "ticket";

  /**
   * CallbackHandler is invoked by TicketLoginModule via JAAS configuration.
   */
  private final CallbackHandler callbackHandler = new CallbackHandler() {

    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      for (int i = 0; i < callbacks.length; i++) {
        if (callbacks[i] instanceof TicketsCallback) {
          TicketsCallback callback = (TicketsCallback) callbacks[i];
          ArrayList<Ticket> tickets = new ArrayList<>();
          ticketQueue.drainTo(tickets);
          callback.setTickets(tickets);
        } else if (callbacks[i] instanceof SessionIdCallback) {
          SessionIdCallback callback = (SessionIdCallback) callbacks[i];
          callback.setSessionId(sessionId);
        } else if (callbacks[i] instanceof NameCallback) {
          NameCallback callback = (NameCallback) callbacks[i];
          callback.setName(entity.getName());
        } else if (callbacks[i] instanceof TicketCallback) {
          TicketCallback callback = (TicketCallback) callbacks[i];
          callback.setTicket(token);
        }
      }
    }
  };

  private UUID sessionId;
  private Entity entity;
  private byte[] token;
  private final BlockingQueue<Ticket> ticketQueue = new LinkedBlockingQueue<>();

  private final Receiver credentialsReceiver = new Receiver() {

    private final CredentialsResponseMessage message = new CredentialsResponseMessage();

    public void accept(ByteBuffer buffer) {
      message.attachForDecode(buffer, 0);
      try {
        String name = message.getName();
        byte[] token = message.getTicket();
        Ticket ticket = new Ticket(token);
        ticketQueue.add(ticket);
      } catch (UnsupportedEncodingException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

  };

  private Subscription credentialsSubsription;


  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.fixp.auth.AbstractAuthenticator#open()
   */
  @Override
  public CompletableFuture<? extends AbstractAuthenticator> open() {
    super.open();

    Topic topic = SessionEventTopics.getTopic(SERVICE_CREDENTIALS_RETRIEVED);
    credentialsSubsription = getReactor().subscribe(topic, credentialsReceiver);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean authenticate(UUID sessionId, byte[] credentials) {

    this.entity = Credentials.getEntity(credentials);
    this.token = Credentials.getToken(credentials);
    this.sessionId = sessionId;
    Subject subject = new Subject();

    try {
      System.setProperty("java.security.auth.login.config", "jaas-ticket.config");
      LoginContext context = new LoginContext(CONFIG_NAME, subject, this.callbackHandler);
      context.login();
      System.out.format("Authenticated session ID=%s credentials=%s\n", sessionId.toString(),
          entity.getName());
      return true;
    } catch (LoginException e) {
      System.out.format("Authentication failed for session ID=%s credentials=%s; %s\n",
          sessionId.toString(), entity.getName(), e.getMessage());
      return false;
    }
  }

}
