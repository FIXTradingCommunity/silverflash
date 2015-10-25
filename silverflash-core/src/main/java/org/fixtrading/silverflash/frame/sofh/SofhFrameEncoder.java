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
package org.fixtrading.silverflash.frame.sofh;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

import org.fixtrading.silverflash.frame.MessageFrameEncoder;

/**
 * FIX Simple Open Framing Header encoder
 * @author Don Mendelson
 *
 */
public class SofhFrameEncoder implements MessageFrameEncoder {
 
  public static final int MESSAGE_LENGTH_OFFSET = 0;
  public static final int ENCODING_OFFSET = 4;
  public static final int HEADER_LENGTH = 6;

  private ByteBuffer buffer;
  private ByteOrder originalByteOrder;
  private int frameStartOffset = 0;
  private int messageLength = -1;
  private short encoding = Encoding.SBE_1_0_LITTLE_ENDIAN.getCode();

  /* (non-Javadoc)
   * @see org.fixtrading.silverflash.buffer.MessageFrameEncoder#encodeFrameHeader()
   */
  @Override
  public MessageFrameEncoder encodeFrameHeader() {
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putInt(frameStartOffset + MESSAGE_LENGTH_OFFSET, messageLength);
    buffer.putShort(frameStartOffset + ENCODING_OFFSET, encoding);
    buffer.position(frameStartOffset + HEADER_LENGTH);
    buffer.order(originalByteOrder);
    return this;
  }

  /* (non-Javadoc)
   * @see org.fixtrading.silverflash.buffer.MessageFrameEncoder#encodeFrameTrailer()
   */
  @Override
  public MessageFrameEncoder encodeFrameTrailer() {
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.putInt(frameStartOffset + MESSAGE_LENGTH_OFFSET, messageLength);
    buffer.position(frameStartOffset + HEADER_LENGTH + messageLength);
    buffer.order(originalByteOrder);
    return this;
  }

  /* (non-Javadoc)
   * @see org.fixtrading.silverflash.buffer.MessageFrameEncoder#setMessageLength(int)
   */
  @Override
  public MessageFrameEncoder setMessageLength(int messageLength) {
    this.messageLength  = messageLength;
    return this;
  }
  
  public MessageFrameEncoder setEncoding(short code) {
    this.encoding  = code;
    return this;
  }

  /* (non-Javadoc)
   * @see org.fixtrading.silverflash.buffer.MessageFrameEncoder#wrap(java.nio.ByteBuffer)
   */
  @Override
  public MessageFrameEncoder wrap(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    this.buffer = buffer;
    this.originalByteOrder = buffer.order();
    this.frameStartOffset = buffer.position();
    messageLength = -1;
    return this;
  }

}
