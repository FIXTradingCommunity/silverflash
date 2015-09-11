package org.fixtrading.silverflash.auth;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.fixtrading.silverflash.auth.Ticket;
import org.fixtrading.silverflash.fixp.auth.SessionIdCallback;
import org.fixtrading.silverflash.fixp.auth.SessionPrincipal;

/**
 * Authentication using JAAS
 * 
 * @author Don Mendelson
 *
 */
public class TicketLoginModule implements LoginModule {

  private CallbackHandler callbackHandler;
  // todo: configure from options
  private Duration expiration = Duration.ofSeconds(10);

  private String name;
  private Map<String, ?> options;
  private final Set<UUID> sessionIds = new HashSet<>();
  private Map<String, ?> sharedState;
  private Subject subject;
  private Ticket ticket;
  private final Set<Ticket> tickets = new HashSet<>();
  private UUID uuid;

  /*
   * (non-Javadoc)
   * 
   * @see javax.security.auth.spi.LoginModule#abort()
   */
  public boolean abort() throws LoginException {
    sessionIds.remove(uuid);
    return true;
  }

  public void addTicket(Ticket ticket) {
    tickets.add(ticket);
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.security.auth.spi.LoginModule#commit()
   */
  public boolean commit() throws LoginException {
    subject.getPrincipals().add(new SessionPrincipal(uuid));
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.security.auth.spi.LoginModule#initialize(javax.security.auth.Subject ,
   * javax.security.auth.callback.CallbackHandler, java.util.Map, java.util.Map)
   */
  public void initialize(Subject subject, CallbackHandler callbackHandler,
      Map<String, ?> sharedState, Map<String, ?> options) {
    this.subject = subject;
    this.callbackHandler = callbackHandler;
    this.sharedState = sharedState;
    this.options = options;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.security.auth.spi.LoginModule#login()
   */
  public boolean login() throws LoginException {

    final Callback[] callbacks = new Callback[4];
    final TicketsCallback ticketsCallback = new TicketsCallback();
    final SessionIdCallback sessionIdCallback = new SessionIdCallback();
    final NameCallback nameCallback = new NameCallback("Noprompt");
    final TicketCallback ticketCallback = new TicketCallback();
    callbacks[0] = ticketsCallback;
    callbacks[1] = sessionIdCallback;
    callbacks[2] = nameCallback;
    callbacks[3] = ticketCallback;

    try {
      callbackHandler.handle(callbacks);
    } catch (IOException | UnsupportedCallbackException e) {
      throw new LoginException(e.getMessage());
    }

    // Add recently generated tickets
    tickets.addAll(ticketsCallback.getTickets());

    this.name = nameCallback.getName();

    if (name == null || name.length() == 0) {
      throw new LoginException("Invalid or empty entity name");
    }

    this.ticket = ticketCallback.getTicket();
    if (ticket == null) {
      throw new LoginException("Missing ticket");
    }

    if (!tickets.remove(ticket)) {
      throw new LoginException("Invalid ticket");
    }

    final Instant issuedAt = ticket.getTimestamp();
    final Instant now = Instant.now();
    final Duration duration = Duration.between(issuedAt, now);
    if (duration.compareTo(expiration) > 0) {
      throw new LoginException("Expired ticket");
    }

    this.uuid = sessionIdCallback.getSessionId();

    if (uuid == null || !sessionIds.add(uuid)) {
      throw new LoginException("Missing or duplicate session ID");
    }
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.security.auth.spi.LoginModule#logout()
   */
  public boolean logout() throws LoginException {
    Set<SessionPrincipal> sessionPrincipals = subject.getPrincipals(SessionPrincipal.class);
    for (SessionPrincipal principal : sessionPrincipals) {
      subject.getPrincipals().remove(principal);
      sessionIds.remove(principal.getUUID());
    }

    return true;
  }

}
