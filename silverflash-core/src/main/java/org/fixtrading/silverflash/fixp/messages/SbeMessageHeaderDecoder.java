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
 * Standard SBE message header decoder
 * 
 * @author Don Mendelson
 * @see <a href="https://github.com/FIXTradingCommunity/fix-simple-binary-encoding">FIX Simple
 *      Binary Encoding</a>
 */
public class SbeMessageHeaderDecoder {

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

  public int getBlockLength() {
    return buffer.getShort(offset + BLOCK_LENGTH_OFFSET) & 0xffff;
  }

  public ByteBuffer getBuffer() {
    return buffer;
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
    return "SbeMessageHeaderDecoder [blockLength=" + getBlockLength() + ", templateId="
        + getTemplateId() + ", schemaId=" + getSchemaId() + ", schemaVersion=" + getSchemaVersion()
        + "]";
  }

  /**
   * Attach a buffer to decode a message header
   * 
   * @param buffer
   *          buffer to decode. Be sure to invoke {@link ByteBuffer#order()} to set byte order to
   *          the value specified in the SBE message schema. For best performance,
   *          {@code ByteOrder.nativeOrder()} is preferred.
   * @param offset
   *          index to a buffer position
   * @return this SbeMessageHeaderDecoder
   * @see java.nio.ByteOrder
   */
  public SbeMessageHeaderDecoder wrap(ByteBuffer buffer, int offset) {
    this.buffer = buffer;
    this.offset = offset;
    return this;
  }

}
