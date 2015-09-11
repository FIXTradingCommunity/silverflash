package org.fixtrading.silverflash.auth;

import org.fixtrading.silverflash.Service;

/**
 * Authentication service
 * 
 * @author Don Mendelson
 *
 * @param T type of session ID
 */
public interface Authenticator<T> extends Service {

  /**
   * Authenticate a session
   * 
   * @param sessionId session identifier
   * @param credentials password or key
   * @return Returns {@code true} if the session was successfully authenticated
   */
  boolean authenticate(T sessionId, byte[] credentials);

}
