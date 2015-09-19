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

package org.fixtrading.silverflash.fixp.messages;

/**
 * Enumeration of rejection reasons for session establishment
 * 
 * @author Don Mendelson
 *
 */
public enum EstablishmentReject {

  /**
   * Establish request was not preceded by a Negotiation or session was finalized, requiring
   * renegotiation.
   */
  UNNEGOTIATED((byte) 0),
  /**
   * EstablishmentAck was already sent; Establish was redundant.
   */
  ALREADY_ESTABLISHED((byte) 1),
  /**
   * User is not authorized.
   */
  SESSION_BLOCKED((byte) 2),
  /**
   * Value is out of accepted range.
   */
  KEEPALIVE_INTERVAL((byte) 3),
  /**
   * Failed authentication because identity is not recognized, keys are invalid, or the user is not
   * authorized to use a particular service.
   */
  CREDENTIALS((byte) 4),
  /**
   * Any other reason that the server cannot establish a session
   */
  UNSPECIFIED((byte) 5);

  private final byte code;

  private EstablishmentReject(byte code) {
    this.code = code;
  }

  public byte getCode() {
    return this.code;
  }

  public static EstablishmentReject getReject(byte code) {
    switch (code) {
      case 0:
        return UNNEGOTIATED;
      case 1:
        return ALREADY_ESTABLISHED;
      case 2:
        return SESSION_BLOCKED;
      case 3:
        return KEEPALIVE_INTERVAL;
      case 4:
        return CREDENTIALS;
      case 5:
        return UNSPECIFIED;
      default:
        throw new RuntimeException("Internal error");
    }
  }
}
