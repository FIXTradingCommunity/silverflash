package org.fixtrading.silverflash.transport;


/**
 * A Consumer with session ID
 * 
 * @author Don Mendelson
 * @param T identifier type
 */
public interface IdentifiableTransportConsumer<T> extends TransportConsumer {

  /**
   * @return a unique session ID
   */
  T getSessionId();
}
