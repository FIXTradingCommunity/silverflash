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

import java.nio.ByteBuffer;

/**
 * Message header consists of a standard SBE header prefixed by message length
 * 
 * @author Don Mendelson
 *
 */
public class MessageHeaderWithFrame {

  /**
   * Encodes a header
   * 
   * @param buffer buffer to populate
   * @param offset offset to beginning of message header
   * @param blockLength SBE block length
   * @param templateId template identifier
   * @param schemaId message schema identifier
   * @param schemaVersion schema version number
   * @param messageLength message length
   */
  public static void encode(ByteBuffer buffer, int offset, int blockLength, int templateId,
      int schemaId, int schemaVersion, int messageLength) {
    buffer.putShort(offset + MESSAGE_LENGTH_OFFSET, (short) messageLength);
    buffer.putShort(offset + BLOCK_LENGTH_OFFSET, (short) blockLength);
    buffer.putShort(offset + TEMPLATE_ID_OFFSET, (short) templateId);
    buffer.putShort(offset + SCHEMA_ID_OFFSET, (short) schemaId);
    buffer.putShort(offset + VERSION_OFFSET, (short) schemaVersion);
    buffer.position(offset + HEADER_LENGTH);
  }

  public static void encodeMessageLength(ByteBuffer buffer, int offset, int messageLength) {
    buffer.putShort(offset + MESSAGE_LENGTH_OFFSET, (short) messageLength);
  }

  public static int getLength() {
    return HEADER_LENGTH;
  }

  private static final int MESSAGE_LENGTH_OFFSET = 0;
  private static final int BLOCK_LENGTH_OFFSET = 2;
  private static final int TEMPLATE_ID_OFFSET = 4;
  private static final int SCHEMA_ID_OFFSET = 6;
  private static final int VERSION_OFFSET = 8;

  private static final int HEADER_LENGTH = 10;

  private ByteBuffer buffer;
  private int offset;

  public MessageHeaderWithFrame attachForDecode(ByteBuffer buffer, int offset) {
    this.buffer = buffer;
    this.offset = offset;
    return this;
  }

  public int getMessageLength() {
    return buffer.getShort(offset + MESSAGE_LENGTH_OFFSET) & 0xffff;
  }

  public int getBlockLength() {
    return buffer.getShort(offset + BLOCK_LENGTH_OFFSET) & 0xffff;
  }

  public int getSchemaId() {
    return buffer.getShort(offset + SCHEMA_ID_OFFSET) & 0xffff;
  }

  public int getSchemaVersion() {
    return buffer.getShort(offset + VERSION_OFFSET) & 0xffff;
  }

  public int getTemplateId() {
    return buffer.getShort(offset + TEMPLATE_ID_OFFSET) & 0xffff;
  }

  @Override
  public String toString() {
    return "MessageHeaderWithFrame [messageLength=" + getMessageLength() + ", blockLength="
        + getBlockLength() + ", templateId=" + getTemplateId() + ", schemaId=" + getSchemaId()
        + ", schemaVersion=" + getSchemaVersion() + "]";
  }

}
