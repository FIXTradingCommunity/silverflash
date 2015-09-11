package org.fixtrading.silverflash.fixp;

import org.fixtrading.silverflash.fixp.messages.FlowType;

/**
 * Initiates a Session when a Transport connection is established
 * 
 * @author Don Mendelson
 *
 */
public interface Establisher {

  /**
   * Transport layer established a connection
   */
  void connected();

  /**
   * @return type of the inbound flow of a Session
   */
  FlowType getInboundFlow();

  /**
   * @return expected inbound heartbeat interval (milliseconds)
   */
  int getInboundKeepaliveInterval();

  /**
   * @return type of the outbound flow of a Session
   */
  FlowType getOutboundFlow();

  /**
   * @return outbound heartbeat interval (milliseconds)
   */
  int getOutboundKeepaliveInterval();

  /**
   * @return Session ID serialized to a byte array
   */
  byte[] getSessionId();

  /**
   * Sets heartbeat interval
   * 
   * @param outboundKeepaliveInterval heartbeat interval in milliseconds
   * @return this Establisher
   */
  Establisher withOutboundKeepaliveInterval(int outboundKeepaliveInterval);

}
