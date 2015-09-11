package org.fixtrading.silverflash.transport;

import java.io.IOException;
import java.util.function.Function;

import org.fixtrading.silverflash.Service;

/**
 * Listens for connections
 * 
 * @author Don Mendelson
 */
public interface Acceptor extends Service {

  /**
   * Returns a function that wraps an accepted Transport into a session of generic type
   * 
   * @param T wrapper class for a Transport
   * @return a function handle accepted connections
   */
  <T> Function<Transport, T> getTransportWrapper();

  /**
   * Invoked when a new connection is ready to accept
   * 
   * @return an object holding the accepted connection
   * @throws IOException if fails to establish the new connection
   */
  Transport readyToAccept() throws IOException;
}
