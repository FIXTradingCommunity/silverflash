package org.fixtrading.silverflash.fixp.auth;

import java.security.Principal;
import java.util.UUID;

import org.fixtrading.silverflash.fixp.SessionId;

/**
 * @author Don Mendelson
 *
 */
public class SessionPrincipal implements Principal {

  private final UUID uuid;

  public SessionPrincipal(byte[] sessionId) {
    this.uuid = SessionId.UUIDFromBytes(sessionId);
  }

  public SessionPrincipal(UUID uuid) {
    this.uuid = uuid;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.security.Principal#getName()
   */
  public String getName() {
    return uuid.toString();
  }

  public UUID getUUID() {
    return uuid;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("SessionPrincipal [");
    if (uuid != null) {
      builder.append("uuid=");
      builder.append(uuid);
    }
    builder.append("]");
    return builder.toString();
  }

}
