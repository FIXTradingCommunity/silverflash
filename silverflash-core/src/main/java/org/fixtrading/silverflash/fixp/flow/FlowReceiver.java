package org.fixtrading.silverflash.fixp.flow;

import org.fixtrading.silverflash.Receiver;

/**
 * Receives messages on FIXP session flow
 * 
 * @author Don Mendelson
 *
 */
public interface FlowReceiver extends Receiver {

  /**
   * Returns whether a heartbeat is past due from a peer. Side effect: resets the value for the next
   * heartbeat interval.
   * 
   * @return Returns {@code true} if a heartbeat is due because neither an application message nor
   *         heartbeat message was received in the last interval.
   */
  boolean isHeartbeatDue();

}
