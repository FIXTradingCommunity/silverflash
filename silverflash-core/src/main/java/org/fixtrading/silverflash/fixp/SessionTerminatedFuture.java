package org.fixtrading.silverflash.fixp;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_SUSPENDED;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.fixtrading.silverflash.reactor.EventFuture;
import org.fixtrading.silverflash.reactor.EventReactor;

/**
 * A Future that notifies an observer when a session has terminated
 * 
 * @author Don Mendelson
 *
 */
public class SessionTerminatedFuture extends EventFuture {

  /**
   * Constructor
   * 
   * @param sessionId session identifier
   * @param reactor an EventReactor
   */
  public SessionTerminatedFuture(UUID sessionId, EventReactor<ByteBuffer> reactor) {
    super(SessionEventTopics.getTopic(sessionId, SESSION_SUSPENDED), reactor);
  }

}
