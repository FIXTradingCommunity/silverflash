package org.fixtrading.silverflash.fixp.auth;

import java.util.UUID;

import javax.security.auth.callback.Callback;

/**
 * A security Callback to acquire Session ID
 * 
 * @author Don Mendelson
 *
 */
public class SessionIdCallback implements Callback {

  private UUID sessionId;

  /**
   * @param sessionId ID of the session
   */
  public void setSessionId(UUID sessionId) {
    this.sessionId = sessionId;
  }

  /**
   * @return the sessionId
   */
  public UUID getSessionId() {
    return sessionId;
  }

}
