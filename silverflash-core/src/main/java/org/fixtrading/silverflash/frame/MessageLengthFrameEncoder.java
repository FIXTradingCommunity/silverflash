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
 * Encodes a message length prefix to delimit a message frame
 * 
 * @author Don Mendelson
 *
 */
public class MessageLengthFrameEncoder implements MessageFrameEncoder {

  private static final int HEADER_LENGTH = 2;
  private static final int MESSAGE_LENGTH_OFFSET = 0;

  private ByteBuffer buffer;
  private int frameStartOffset = 0;
  private long messageLength = -1;

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.buffer.MessageFrameEncoder#encodeFrameHeader()
   */
  @Override
  public MessageFrameEncoder encodeFrameHeader() {
    buffer.putShort(frameStartOffset + MESSAGE_LENGTH_OFFSET, (short) (messageLength & 0xffffffff));
    buffer.position(frameStartOffset + HEADER_LENGTH);
    return this;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.buffer.MessageFrameEncoder#encodeFrameTrailer()
   */
  @Override
  public MessageFrameEncoder encodeFrameTrailer() {
    buffer.putShort(frameStartOffset + MESSAGE_LENGTH_OFFSET, (short) (messageLength & 0xffffffff));
    buffer.position(frameStartOffset + HEADER_LENGTH + (int) messageLength);
    return this;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.buffer.MessageFrameEncoder#setMessageLength(int)
   */
  @Override
  public MessageFrameEncoder setMessageLength(long messageLength) {
    this.messageLength = messageLength;
    return this;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.buffer.MessageFrameEncoder#wrap(java.nio.ByteBuffer)
   */
  @Override
  public MessageFrameEncoder wrap(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    this.buffer = buffer;
    this.frameStartOffset = buffer.position();
    messageLength = -1;
    return this;
  }

  /* (non-Javadoc)
   * @see org.fixtrading.silverflash.frame.MessageFrameEncoder#getHeaderLength()
   */
  @Override
  public int getHeaderLength() {
    return HEADER_LENGTH;
  }

}
