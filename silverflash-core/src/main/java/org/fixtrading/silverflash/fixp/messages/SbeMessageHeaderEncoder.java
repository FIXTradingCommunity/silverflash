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
 * Standard SBE message header encoder
 * 
 * @author Don Mendelson
 * @see <a href="https://github.com/FIXTradingCommunity/fix-simple-binary-encoding">FIX Simple
 *      Binary Encoding</a>
 */
public class SbeMessageHeaderEncoder {

  private static final int BLOCK_LENGTH_OFFSET = 0;
  private static final int HEADER_LENGTH = 8;
  private static final int SCHEMA_ID_OFFSET = 4;
  private static final int TEMPLATE_ID_OFFSET = 2;
  private static final int VERSION_OFFSET = 6;

  public static int getLength() {
    return HEADER_LENGTH;
  }

  private ByteBuffer buffer;
  private int offset;

  /**
   * Encode schema version and set buffer position to start of message body
   * 
   * @param schemaVersion
   *          version of an SBE message schema
   * @return this SbeMessageHeaderEncoder
   */
  public SbeMessageHeaderEncoder getSchemaVersion(int schemaVersion) {
    buffer.putShort(offset + VERSION_OFFSET, (short) schemaVersion);
    buffer.position(offset + HEADER_LENGTH);
    return this;
  }

  /**
   * Encode block length
   * 
   * @param blockLength
   *          length of the root block as specified by an SBE message schema
   * @return this SbeMessageHeaderEncoder
   */
  public SbeMessageHeaderEncoder setBlockLength(int blockLength) {
    buffer.putShort(offset + BLOCK_LENGTH_OFFSET, (short) blockLength);
    return this;
  }

  /**
   * Encode schema ID
   * 
   * @param schemaId
   *          ID of an SBE message schema
   * @return this SbeMessageHeaderEncoder
   */
  public SbeMessageHeaderEncoder setSchemaId(int schemaId) {
    buffer.putShort(offset + SCHEMA_ID_OFFSET, (short) schemaId);
    return this;
  }

  /**
   * Encode a message template ID
   * 
   * @param templateId
   *          ID of an SBE message template
   * @return this SbeMessageHeaderEncoder
   */
  public SbeMessageHeaderEncoder setTemplateId(int templateId) {
    buffer.putShort(offset + TEMPLATE_ID_OFFSET, (short) templateId);
    return this;
  }

  /**
   * Attach a buffer to encode a message header
   * 
   * @param buffer
   *          buffer to encode. Be sure to invoke {@link ByteBuffer#order()} to set byte order to
   *          the value specified in the SBE message schema. For best performance,
   *          {@code ByteOrder.nativeOrder()} is preferred.
   * @param offset
   *          index to a buffer position
   * @return this SbeMessageHeaderEncoder
   * @see java.nio.ByteOrder
   */
  public SbeMessageHeaderEncoder wrap(ByteBuffer buffer, int offset) {
    this.buffer = buffer;
    this.offset = offset;
    return this;
  }
}
