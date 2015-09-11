package org.fixtrading.silverflash.fixp;

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
