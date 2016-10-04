/**
 *    Copyright 2015-2016 FIX Protocol Ltd
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

package io.fixprotocol.silverflash.fixp;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

/**
 * Operations on session IDs
 * 
 * @author Don Mendelson
 *
 */
public final class SessionId {

  /**
   * An uninitialized ID
   */
  public final static UUID EMPTY = new UUID(0L, 0L);

  public static UUID generateUUID() {
    return UUID.randomUUID();
  }

  /**
   * Serialize a session ID
   * 
   * @param uuid unique session ID
   * @return a populated byte array
   */
  public static byte[] UUIDAsBytes(UUID uuid) {
    Objects.requireNonNull(uuid);
    final byte[] sessionId = new byte[16];
    // UUID is big-endian according to standard, which is the default byte
    // order of ByteBuffer
    final ByteBuffer b = ByteBuffer.wrap(sessionId);
    b.putLong(0, uuid.getMostSignificantBits());
    b.putLong(8, uuid.getLeastSignificantBits());
    return sessionId;
  }

  /**
   * Deserialize a session ID
   * 
   * @param sessionId an ID in a buffer
   * @return a unique ID object
   */
  public static UUID UUIDFromBytes(byte[] sessionId) {
    Objects.requireNonNull(sessionId);
    if (sessionId.length < 16) {
      throw new IllegalArgumentException("Length must be 16");
    }
    final ByteBuffer b = ByteBuffer.wrap(sessionId);
    long mostSigBits = b.getLong(0);
    long leastSigBits = b.getLong(8);

    return new UUID(mostSigBits, leastSigBits);
  }

}
