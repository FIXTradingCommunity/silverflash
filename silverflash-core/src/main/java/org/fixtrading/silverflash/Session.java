package org.fixtrading.silverflash;

import java.io.IOException;

/**
 * An instance of a session layer to exchange messages between peers (OSI layer 5)
 * 
 * @author Don Mendelson
 * 
 * @param T identifier type
 */
public interface Session<T> extends Sender, AutoCloseable {

  /**
   * @return a unique session ID
   */
  T getSessionId();

  /**
   * Initialize this Session. Open a Transport and acquire any other needed resources.
   * 
   * @throws IOException if an IO error occurs
   */
  void open() throws IOException;

}
