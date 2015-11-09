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
 * Enumeration of reject reasons for session negotiation
 * 
 * @author Don Mendelson
 *
 */
public enum NegotiationReject {

  /**
   * Failed authentication because identity is not recognized, keys are invalid, or the user is not
   * authorized to use a particular service
   */
  CREDENTIALS((byte) 0),
  /**
   * Any other reason that the server cannot create a session
   */
  UNSPECIFIED((byte) 1),
  /**
   * Server does not support requested client flow type.
   */
  FLOW_TYPE_NOT_SUPPORTED((byte) 2),
  /**
   * FixpSession ID is non-unique
   */
  DUPLICATE_ID((byte) 3);

  private final byte code;

  NegotiationReject(byte code) {
    this.code = code;
  }

  public byte getCode() {
    return this.code;
  }

  public static NegotiationReject getReject(byte code) {
    switch (code) {
      case 0:
        return CREDENTIALS;
      case 1:
        return UNSPECIFIED;
      case 2:
        return FLOW_TYPE_NOT_SUPPORTED;
      case 3:
        return DUPLICATE_ID;
      default:
        throw new RuntimeException("Internal error");
    }
  }
}
