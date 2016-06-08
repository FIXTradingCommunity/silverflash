/**
 * Copyright 2015 FIX Protocol Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.fixtrading.silverflash.fixp.messages;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Function;

import org.fixtrading.silverflash.fixp.SessionId;

/**
 * Retrieves session ID from a FIXP session message
 * 
 * Optimized to take advantage of change in FIXP RC3 to always put session ID in
 * same position in session messages.
 * 
 * @author Don Mendelson
 *
 */
public class MessageSessionIdentifier implements Function<ByteBuffer, UUID> {

  private final SbeMessageHeaderDecoder headerDecoder = new SbeMessageHeaderDecoder();
  private final byte[] sessionId = new byte[16];

  /*
   * (non-Javadoc)
   * 
   * @see java.util.function.Function#apply(java.lang.Object)
   */
  public UUID apply(ByteBuffer buffer) {
    UUID uuid = null;
    int pos = buffer.position();
    try {
      final MessageType msgType = getMessageType(buffer, pos);
      if (msgType == null) {
        // Must be an application message
        return null;
      }
      
      switch (msgType) {
        case APPLIED:
        case NOT_APPLIED:
        case SEQUENCE:
        case UNSEQUENCED_HEARTBEAT:
          // Returns null.
          break;
        case NEGOTIATE:
        case NEGOTIATION_RESPONSE:
        case NEGOTIATION_REJECT:
        case ESTABLISH:
        case ESTABLISHMENT_ACK:
        case ESTABLISHMENT_REJECT:
        case TERMINATE:
        case RETRANSMIT_REQUEST:
        case RETRANSMISSION:
        case FINISHED_SENDING:
        case FINISHED_RECEIVING:
        case CONTEXT:
          getSessionId(buffer, pos+SbeMessageHeaderDecoder.getLength(), sessionId, 0);
          uuid = SessionId.UUIDFromBytes(sessionId);
          break;
        default:
          return null;
      }

      return uuid;
    } finally {
      // Idempotent for buffer position
      buffer.position(pos);
    }
  }

  private MessageType getMessageType(ByteBuffer buffer, int offset) {
    int schema = headerDecoder.wrap(buffer, offset).getSchemaId();
    if (schema != SessionMessageSchema.SCHEMA_ID) {
      return null;
    }
    int code = headerDecoder.getTemplateId();

    return MessageType.getMsgType(code);
  }

  private void getSessionId(ByteBuffer buffer, int offset, byte[] dest, int destOffset) {
    buffer.position(offset);
    buffer.get(dest, destOffset, 16);
  }
}
