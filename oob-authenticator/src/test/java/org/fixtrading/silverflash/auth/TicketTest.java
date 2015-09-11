package org.fixtrading.silverflash.auth;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;
import java.security.SecureRandom;

import org.fixtrading.silverflash.auth.Ticket;
import org.fixtrading.silverflash.util.BufferDumper;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Don Mendelson
 *
 */
public class TicketTest {

  private SecureRandom random;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    random = new SecureRandom();
  }


  /**
   * Test method for
   * {@link org.fixtrading.silverflash.auth.Ticket#generateTicket(byte[], java.security.SecureRandom, java.lang.String)}
   * .
   * 
   * @throws UnsupportedEncodingException
   */
  @Test
  public void testGenerateTicket() throws UnsupportedEncodingException {
    final String authzId = "Firm999";
    byte[] token = Ticket.generateTicket(random, authzId);
    // BufferDumper.print(token, 16, token.length, System.out);

    final Ticket ticket = new Ticket(token);
    byte[] bytes = ticket.getBytes();
    BufferDumper.print(bytes, 16, bytes.length, System.out);

    assertEquals(authzId, ticket.getAuthzId());
    assertTrue(ticket.getTimestamp().toString().endsWith("Z"));
    assertEquals(32, ticket.getNonce().length);
    assertEquals(32, ticket.getResponseToken().length);
  }

}
