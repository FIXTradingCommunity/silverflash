/**
 * Copyright 2015-2016 FIX Protocol Ltd
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
package org.fixtrading.silverflash.frame;

import java.nio.ByteBuffer;

/**
 * Encodes a frame to delimit a message
 * 
 * @author Don Mendelson
 *
 */
public interface MessageFrameEncoder {
  
  /**
   * Returns a new MessageFrameEncoder of the same class
   * @return a new MessageFrameEncoder
   */
  MessageFrameEncoder copy();

  /**
   * Encode a header that delimits the beginning of a frame
   * 
   * @return this MessageFrameEncoder
   */
  MessageFrameEncoder encodeFrameHeader();

  /**
   * Encode a trailer that delimits the end of a frame. Optional implementation, depending on
   * protocol.
   * 
   * @return this MessageFrameEncoder
   */
  MessageFrameEncoder encodeFrameTrailer();

  /**
   * Returns the length of the message frame, including header and trailer
   * 
   * @return the length of the frame
   */
  long getEncodedLength();

  /**
   * Returns the length of a frame header
   * 
   * @return the length of a header
   */
  int getHeaderLength();

  /**
   * Sets the message length in the message frame header or trailer. Optional implementation,
   * depending on protocol. It is required if message length must be encoded in the header before
   * the message is encoded.
   * 
   * @param messageLength size of the message contained by this frame
   * @return this MessageFrameEncoder
   */
  MessageFrameEncoder setMessageLength(long messageLength);

  /**
   * Attach a buffer to encode a message frame.
   * <p>
   * This encoder may use a different byte order than the contained message. Therefore, it must
   * restore the original byte order of the buffer after encoding the frame header and trailer.
   * 
   * @param buffer buffer to populate.
   * @param offset index to start of frame in buffer
   * @return this MessageFrameEncoder
   */
  MessageFrameEncoder wrap(ByteBuffer buffer, int offset);
}
