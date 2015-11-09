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
 * Type of message flow
 * 
 * @author Don Mendelson
 *
 */
public enum FlowType {

  /**
   * Unsequenced flow
   */
  UNSEQUENCED((byte) 0),
  /**
   * Idempotent flow
   */
  IDEMPOTENT((byte) 1),
  /**
   * Recoverable flow
   */
  RECOVERABLE((byte) 2),
  /**
   * No application messages flow in this direction. Part of a one-way session.
   */
  NONE((byte) 3);

  private final byte code;

  FlowType(byte code) {
    this.code = code;
  }

  public byte getCode() {
    return this.code;
  }

  public static FlowType getFlowType(byte code) {
    switch (code) {
      case 0:
        return UNSEQUENCED;
      case 1:
        return IDEMPOTENT;
      case 2:
        return RECOVERABLE;
      case 3:
        return NONE;
      default:
        throw new RuntimeException("Internal error");
    }
  }
}
