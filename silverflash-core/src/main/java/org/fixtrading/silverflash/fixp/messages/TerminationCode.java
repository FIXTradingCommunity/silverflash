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
 * Reasons for terminating a session
 * 
 * @author Don Mendelson
 *
 */
public enum TerminationCode {

  /**
   * Finished sending
   */
  FINISHED((byte) 0),
  /**
   * Unknown error
   */
  UNSPECIFIED_ERROR((byte) 1),
  /**
   * Ranged of messages not available for retransmisstion
   */
  RE_REQUEST_OUT_OF_BOUNDS((byte) 2),
  /**
   * Concurrent RetransmitRequest received
   */
  RE_REQUEST_IN_PROGRESS((byte) 3);

  private final byte code;

  private TerminationCode(byte code) {
    this.code = code;
  }

  public byte getCode() {
    return this.code;
  }

  public static TerminationCode getTerminateCode(byte code) {
    switch (code) {
      case 0:
        return FINISHED;
      case 1:
        return UNSPECIFIED_ERROR;
      case 2:
        return RE_REQUEST_OUT_OF_BOUNDS;
      case 3:
        return RE_REQUEST_IN_PROGRESS;
      default:
        throw new RuntimeException("Internal error");
    }
  }
}
