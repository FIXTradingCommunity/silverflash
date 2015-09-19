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
 * Enumeration of protocol message types and their wire identifiers
 * 
 * @author Don Mendelson
 *
 */
public enum MessageType {

  /**
   * Start a sequence of messages
   */
  SEQUENCE(0xffff),
  /**
   * Initiate a new session
   */
  NEGOTIATE(0xfffe),
  /**
   * Accept a new session
   */
  NEGOTIATION_RESPONSE(0xfffd),
  /**
   * Reject a new session request
   */
  NEGOTIATION_REJECT(0xfffc),
  /**
   * Bind a session to a transport
   */
  ESTABLISH(0xfffb),
  /**
   * Accept transport binding
   */
  ESTABLISHMENT_ACK(0xfffa),
  /**
   * Reject transport binding
   */
  ESTABLISHMENT_REJECT(0xfff9),
  /**
   * TERMINATE transport binding
   */
  TERMINATE(0xfff8),
  /**
   * Heartbeat for an unsequenced flow
   */
  UNSEQUENCED_HEARTBEAT(0xfff7),
  /**
   * Request retransmission on a recoverable flow
   */
  RETRANSMIT_REQUEST(0xfff6),
  /**
   * Request retransmission on a recoverable flow
   */
  RETRANSMISSION(0xfff5),
  /**
   * Last application message was sent
   */
  FINISHED_SENDING(0xfff4),
  /**
   * Last application message has been processed
   */
  FINISHED_RECEIVING(0xfff3),
  /**
   * Start a new sequence on a multiplexed session
   */
  CONTEXT(0xfff2),
  /**
   * Acknowledge that an application message has been processed
   */
  APPLIED(0xfff1),
  /**
   * Notification that an application message has not been processed
   */
  NOT_APPLIED(0xfff0);

  private final int code;

  private MessageType(int code) {
    this.code = code;
  }

  public int getCode() {
    return this.code;
  }

  public static MessageType getMsgType(int code) {
    switch (code) {
      case 0xffff:
        return SEQUENCE;
      case 0xfffe:
        return NEGOTIATE;
      case 0xfffd:
        return NEGOTIATION_RESPONSE;
      case 0xfffc:
        return NEGOTIATION_REJECT;
      case 0xfffb:
        return ESTABLISH;
      case 0xfffa:
        return ESTABLISHMENT_ACK;
      case 0xfff9:
        return ESTABLISHMENT_REJECT;
      case 0xfff8:
        return TERMINATE;
      case 0xfff7:
        return UNSEQUENCED_HEARTBEAT;
      case 0xfff6:
        return RETRANSMIT_REQUEST;
      case 0xfff5:
        return RETRANSMISSION;
      case 0xfff4:
        return FINISHED_SENDING;
      case 0xfff3:
        return FINISHED_RECEIVING;
      case 0xfff2:
        return CONTEXT;
      case 0xfff1:
        return APPLIED;
      case 0xfff0:
        return NOT_APPLIED;
      default:
        throw new RuntimeException("Internal error");
    }
  }

}
