package org.fixtrading.silverflash.reactor;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import org.fixtrading.silverflash.Receiver;

/**
 * A Future to hold asynchronous results
 * 
 * @author Don Mendelson
 *
 */
public class EventFuture extends CompletableFuture<ByteBuffer> {

  private final Receiver receiver = new Receiver() {

    public void accept(ByteBuffer buffer) {
      complete(buffer);
      subscription.unsubscribe();
    }

  };

  private Subscription subscription;

  /**
   * Constructor
   * 
   * @param topic a Topic of interest
   * @param reactor an EventReactor
   */
  public EventFuture(Topic topic, EventReactor<ByteBuffer> reactor) {
    subscription = reactor.subscribe(topic, receiver);
  }
}
