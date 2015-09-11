package org.fixtrading.silverflash.auth;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;

/**
 * A security ticket to prove a claim to identity
 * <p>
 * Format: '<' authzId (space) timestamp (space) nonce (space) response-token '>'
 * <p>
 * Timestamp is current time in millis as a hex string.
 * @author Don Mendelson
 */
public class Ticket {

  private static final char BEGIN_CHAR = '<';
  private static final byte DELIMITER_CHAR = ' ';
  private static final char END_CHAR = '>';
  private static final int TOKEN_LENGTH = 32;

  /**
   * Generate a ticket
   * 
   * @param random random number generator
   * @param authzId authorization ID of entity
   * @return length of the ticket
   */
  public static byte[] generateTicket(SecureRandom random, String authzId) {
    byte[] bytes = new byte[1024];

    final byte[] nonce = new byte[TOKEN_LENGTH];
    generateSalt(random, nonce);
    final byte[] response = new byte[TOKEN_LENGTH];
    generateSalt(random, response);
    final Instant utcTime = Instant.now();

    bytes[0] = BEGIN_CHAR;
    int length = 1;
    final byte[] authzIdBytes = authzId.getBytes(StandardCharsets.UTF_8);
    System.arraycopy(authzIdBytes, 0, bytes, length, authzIdBytes.length);
    length += authzIdBytes.length;
    bytes[length] = DELIMITER_CHAR;
    length++;

    final byte[] timeBytes = Long.toString(utcTime.getEpochSecond()).getBytes();
    System.arraycopy(timeBytes, 0, bytes, length, timeBytes.length);
    length += timeBytes.length;
    bytes[length] = DELIMITER_CHAR;
    length++;

    System.arraycopy(nonce, 0, bytes, length, nonce.length);
    length += nonce.length;
    bytes[length] = DELIMITER_CHAR;
    length++;

    System.arraycopy(response, 0, bytes, length, response.length);
    length += response.length;
    bytes[length] = END_CHAR;
    length++;

    final Encoder encoder = Base64.getEncoder();
    byte[] encodedBytes = new byte[2048];
    int encodedLength = encoder.encode(Arrays.copyOf(bytes, length), encodedBytes);
    return Arrays.copyOf(encodedBytes, encodedLength);
  }

  private static byte[] generateSalt(SecureRandom random, byte[] salt) {
    random.nextBytes(salt);
    return salt;
  }

  private final byte[] bytes;
  private final Decoder decoder;

  public Ticket(byte[] src) {
    decoder = Base64.getDecoder();
    this.bytes = decoder.decode(src);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof Ticket))
      return false;
    Ticket other = (Ticket) obj;
    return Arrays.equals(bytes, other.bytes);
  }

  public String getAuthzId() {
    int offset = 0;
    for (; offset < bytes.length; offset++) {
      if (bytes[offset] == BEGIN_CHAR) {
        break;
      }
    }
    offset++;
    for (int i = offset; i < bytes.length; i++) {
      if (bytes[i] == DELIMITER_CHAR) {
        return new String(bytes, offset, i - offset, StandardCharsets.UTF_8);
      }
    }
    return null;
  }

  /**
   * Returns a nonce contained by this Ticket
   * 
   * @return a random array of bytes
   */
  public byte[] getNonce() {
    int offset = 0;
    int field = 0;
    for (; offset < bytes.length && field < 2; offset++) {
      if (bytes[offset] == DELIMITER_CHAR) {
        field++;
      }
    }

    for (int i = bytes.length - 1; i >= offset; i--) {
      if (bytes[i] == DELIMITER_CHAR) {
        return Arrays.copyOfRange(bytes, offset, i);
      }
    }
    return null;
  }

  /**
   * Returns a response token contained by this Ticket
   * 
   * @return a random array of bytes
   */
  public byte[] getResponseToken() {
    int offset = 0;
    int field = 0;
    for (; offset < bytes.length && field < 3; offset++) {
      if (bytes[offset] == DELIMITER_CHAR) {
        field++;
      }
    }

    for (int i = bytes.length - 1; i >= offset; i--) {
      if (bytes[i] == END_CHAR) {
        return Arrays.copyOfRange(bytes, offset, i);
      }
    }
    return null;
  }

  /**
   * Returns a timestamp of this Ticket
   * 
   * @return the time that the ticket was generated
   */
  public Instant getTimestamp() {
    int offset = 0;
    for (; offset < bytes.length; offset++) {
      if (bytes[offset] == DELIMITER_CHAR) {
        offset++;
        break;
      }
    }
    for (int i = offset; i < bytes.length; i++) {
      if (bytes[i] == DELIMITER_CHAR) {
        String time = new String(bytes, offset, i - offset, StandardCharsets.UTF_8);
        long epochSecond = Long.parseLong(time);
        return Instant.ofEpochSecond(epochSecond);
      }
    }
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(bytes);
    return result;
  }

  /**
   * @return the bytes
   */
  byte[] getBytes() {
    return bytes;
  }

}
