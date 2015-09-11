package org.fixtrading.silverflash.reactor;


import java.io.IOException;

import org.fixtrading.silverflash.Receiver;

/**
 * Dispatches events to a receiver
 * 
 * @author Don Mendelson
 *
 */
@FunctionalInterface
public interface Dispatcher<T> {

  /**
   * Dispatch an event
   * 
   * @param topic category of the event
   * @param payload a message payload
   * @param receiver target of the event
   * @throws IOException if an IO error occurs
   */
  void dispatch(Topic topic, T payload, Receiver receiver) throws IOException;
}
