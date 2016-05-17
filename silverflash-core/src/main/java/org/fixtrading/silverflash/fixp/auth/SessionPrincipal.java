/**
 *    Copyright 2015 FIX Protocol Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.fixtrading.silverflash.fixp.auth;

import java.security.Principal;
import java.util.UUID;

import org.fixtrading.silverflash.fixp.SessionId;

/**
 * Represents the owner of a FIXP session
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

  /**
   * Returns session ID
   * @return a unique identifier
   */
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
