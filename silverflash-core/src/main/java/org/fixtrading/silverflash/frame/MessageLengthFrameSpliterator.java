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
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Splits and iterates message frames within a buffer
 * <p>
 * Framing protocol expects an SBE message header with a message length prefix to delimit messages.
 * 
 * @author Don Mendelson
 *
 */
public class MessageLengthFrameSpliterator implements FrameSpliterator {

  private ByteBuffer buffer;
  private final MessageLengthFrameDecoder decoder = new MessageLengthFrameDecoder();
  private int offset;
  private ByteOrder originalByteOrder;

  /**
   * Construct this FixpWithMessageLengthFrameSpliterator without a buffer Must call
   * {@link #wrap(ByteBuffer)} to use this object.
   */
  public MessageLengthFrameSpliterator() {
  }

  /**
   * Construct this FixpWithMessageLengthFrameSpliterator with a buffer
   * 
   * @param buffer
   *          buffer holding message frames
   */
  public MessageLengthFrameSpliterator(ByteBuffer buffer) {
    wrap(buffer);
  }

  @Override
  public int characteristics() {
    return DISTINCT | NONNULL | IMMUTABLE;
  }

  @Override
  public long estimateSize() {
    return buffer.hasRemaining() ? Long.MAX_VALUE : 0;
  }

  public boolean hasRemaining() {
    return buffer.remaining() > 0;
  }

  @Override
  public boolean tryAdvance(Consumer<? super ByteBuffer> action) {
    Objects.requireNonNull(action);
    int messageOffset = offset + MessageLengthFrameDecoder.HEADER_LENGTH;
    if (messageOffset > buffer.limit()) {
      return false;
    }
    decoder.wrap(buffer);
    decoder.decodeFrameHeader();
    final int messageLength = decoder.getMessageLength();
    int messageLimit = offset + MessageLengthFrameDecoder.HEADER_LENGTH + messageLength;

    if (messageLength <= 0 || (messageLimit > buffer.limit())) {
      return false;
    }

    ByteBuffer message = buffer.duplicate();
    message.order(originalByteOrder);
    message.limit(messageLimit);

    action.accept(message);
    offset = messageLimit; 
    buffer.position(messageLimit);
    return true;
  }

  @Override
  public Spliterator<ByteBuffer> trySplit() {
    return null;
  }

  /**
   * Set a buffer to split into frames. Assumes that the first frame is at current position in the
   * buffer.
   * 
   * @param buffer
   *          buffer holding message frames
   */
  public void wrap(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    this.buffer = buffer;
    this.offset = buffer.position();
    this.originalByteOrder = buffer.order();
  }

}
