package org.fixtrading.silverflash.transport;

/**
 * Connection readiness operation
 * 
 * @author Don Mendelson
 *
 */
public interface Connector {

  /**
   * Invoked when ready to connect. It is the responsibility of a Connector to successfully complete
   * the connection or to report failure.
   */
  void readyToConnect();

}
