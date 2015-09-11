package org.fixtrading.silverflash.auth;

import java.util.Arrays;

/**
 * Parses credentials to extract an Entity
 * 
 * @author Don Mendelson
 *
 */
public final class Credentials {

  private static final String EMPTY_NAME = "";
  private static final byte[] EMPTY_TOKEN = new byte[0];
  private static final byte DELIMITER = (byte) 0;

  /**
   * Parse Principal from credentials passed in session negotiation
   * 
   * @param credentials byte array containing credentials
   * @return a business entity, possibly with an empty name if it cannot be found in credentials
   */
  public static Entity getEntity(byte[] credentials) {
    for (int i = 0; i < credentials.length; i++) {
      if (DELIMITER == credentials[i]) {
        return new Entity(new String(credentials, 0, i));
      }
    }

    return new Entity(EMPTY_NAME);
  }

  /**
   * Parse a security token from credentials passed in session negotiation
   * 
   * @param credentials byte array containing credentials
   * @return a token, or an empty byte array if the token is not found
   */
  public static byte[] getToken(byte[] credentials) {
    for (int i = 0; i < credentials.length; i++) {
      if (DELIMITER == credentials[i]) {
        return Arrays.copyOfRange(credentials, i + 1, credentials.length);
      }
    }

    return EMPTY_TOKEN;
  }

  public static int encode(Entity entity, byte[] token, byte[] credentials) {
    byte[] name = entity.getName().getBytes();
    System.arraycopy(name, 0, credentials, 0, name.length);
    int length = name.length;
    credentials[length] = DELIMITER;
    length++;
    System.arraycopy(token, 0, credentials, length, token.length);
    length += token.length;
    return length;
  }
}
