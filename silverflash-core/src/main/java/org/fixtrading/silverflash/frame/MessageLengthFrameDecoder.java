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
package org.fixtrading.silverflash.frame;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Decodes a message length prefix to delimit a message frame
 * 
 * @author Don Mendelson
 *
 */
public class MessageLengthFrameDecoder implements MessageFrameDecoder {

  public static final int HEADER_LENGTH = 2;
  public static final int MESSAGE_LENGTH_OFFSET = 0;

  private ByteBuffer buffer;
  private int frameStartOffset = 0;
  private int messageLength = -1;

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.frame.sofh.MessageFrameDecoder#decodeFrameHeader()
   */
  @Override
  public MessageFrameDecoder decodeFrameHeader() {
    messageLength = buffer.getShort(frameStartOffset + MESSAGE_LENGTH_OFFSET) & 0xffff;
    buffer.position(frameStartOffset + HEADER_LENGTH);
    return this;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.frame.sofh.MessageFrameDecoder#decodeFrameTrailer()
   */
  @Override
  public MessageFrameDecoder decodeFrameTrailer() {
    return this;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.frame.sofh.MessageFrameDecoder#getMessageLength()
   */
  @Override
  public int getMessageLength() {
    return this.messageLength;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.frame.sofh.MessageFrameDecoder#wrap(java.nio.ByteBuffer)
   */
  @Override
  public MessageFrameDecoder wrap(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    this.buffer = buffer;
    this.frameStartOffset = buffer.position();
    messageLength = -1;
    return this;
  }

}
