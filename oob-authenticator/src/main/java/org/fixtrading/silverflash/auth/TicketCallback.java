package org.fixtrading.silverflash.auth;

import javax.security.auth.callback.Callback;

/**
 * A security Callback to acquire a Ticket
 * 
 * @author Don Mendelson
 *
 */
public class TicketCallback implements Callback {

  private Ticket ticket;

  /**
   * Set a Ticket
   * 
   * @param bytes a Ticket as a byte array
   */
  public void setTicket(byte[] bytes) {
    this.ticket = new Ticket(bytes);
  }

  /**
   * Returns a Ticket
   * 
   * @return Ticket
   */
  public Ticket getTicket() {
    return ticket;
  }

}
