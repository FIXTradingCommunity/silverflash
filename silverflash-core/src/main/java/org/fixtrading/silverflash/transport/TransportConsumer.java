package org.fixtrading.silverflash.transport;

import org.fixtrading.silverflash.Receiver;

/**
 * A consumer of Transport events
 * 
 * @author Don Mendelson
 *
 */
public interface TransportConsumer extends Receiver {

  /**
   * The Transport connected
   */
  void connected();

  /**
   * The Transport disconnected
   */
  void disconnected();
}
