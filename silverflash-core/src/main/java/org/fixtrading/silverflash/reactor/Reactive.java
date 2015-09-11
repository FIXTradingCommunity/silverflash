package org.fixtrading.silverflash.reactor;

/**
 * A service that can react to events
 * 
 * @author Don Mendelson
 *
 * @param <T> type of event payload
 */
public interface Reactive<T> {

  /**
   * Provide an EventReactor
   * 
   * @param reactor an EventReactor
   * @return this object
   */
  Reactive<T> withEventReactor(EventReactor<T> reactor);
}
