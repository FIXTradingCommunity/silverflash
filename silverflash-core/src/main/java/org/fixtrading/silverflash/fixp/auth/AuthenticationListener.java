package org.fixtrading.silverflash.fixp.auth;

import java.util.UUID;

/**
 * Listen for Authenticator results
 * 
 * @author Don Mendelson
 *
 */
public interface AuthenticationListener {

  /**
   * Client was authenticated
   * 
   * @param sessionId session identifier
   */
  void authenticated(UUID sessionId);

  /**
   * Client was denied access
   * 
   * @param sessionId session identifier
   */
  void authenticationFailed(UUID sessionId);
}
