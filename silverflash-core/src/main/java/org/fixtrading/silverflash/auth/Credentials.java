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
