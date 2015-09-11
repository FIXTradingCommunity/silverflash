package org.fixtrading.silverflash.auth;

import java.util.List;

import javax.security.auth.callback.Callback;

import org.fixtrading.silverflash.auth.Ticket;

/**
 * A security Callback to retrieve recent Tickets
 * 
 * @author Don Mendelson
 *
 */
public class TicketsCallback implements Callback {

  private List<Ticket> tickets;

  public void setTickets(List<Ticket> tickets) {
    this.tickets = tickets;
  }

  public List<Ticket> getTickets() {
    return tickets;
  }

}
