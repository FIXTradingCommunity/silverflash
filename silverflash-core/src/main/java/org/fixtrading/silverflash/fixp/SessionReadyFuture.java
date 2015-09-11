package org.fixtrading.silverflash.fixp;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_READY;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.fixtrading.silverflash.reactor.EventFuture;
import org.fixtrading.silverflash.reactor.EventReactor;

/**
 * A Future that notifies an observer when a session is ready to use
 * 
 * @author Don Mendelson
 *
 */
public class SessionReadyFuture extends EventFuture {

  /**
   * Constructor
   * 
   * @param sessionId session identifier
   * @param reactor an EventReactor
   */
  public SessionReadyFuture(UUID sessionId, EventReactor<ByteBuffer> reactor) {
    super(SessionEventTopics.getTopic(sessionId, SESSION_READY), reactor);
  }

}
