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

package org.fixtrading.silverflash.fixp.frame;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

import org.fixtrading.silverflash.buffer.FrameSpliterator;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderWithFrame;

/**
 * Splits and iterates message frames within a buffer
 * <p>
 * Framing protocol expects an SBE message header with a message length prefix to delimit messages.
 * 
 * @author Don Mendelson
 *
 */
public class FixpWithMessageLengthFrameSpliterator implements FrameSpliterator {

  private ByteBuffer buffer;
  private int offset = 0;
  private final MessageHeaderWithFrame messageHeader = new MessageHeaderWithFrame();

  /**
   * Construct this FixpWithMessageLengthFrameSpliterator without a buffer Must call
   * {@link #wrap(ByteBuffer)} to use this object.
   */
  public FixpWithMessageLengthFrameSpliterator() {}

  /**
   * Construct this FixpWithMessageLengthFrameSpliterator with a buffer
   * 
   * @param buffer buffer holding message frames
   */
  public FixpWithMessageLengthFrameSpliterator(ByteBuffer buffer) {
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

  /**
   * Set a buffer to split into frames. Assumes that the first frame is at current position in the
   * buffer.
   * 
   * @param buffer buffer holding message frames
   */
  public void wrap(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    this.buffer = buffer;
    offset = buffer.position();
  }

  @Override
  public boolean tryAdvance(Consumer<? super ByteBuffer> action) {
    Objects.requireNonNull(action);
    if (offset + MessageHeaderWithFrame.getLength() > buffer.limit()) {
      return false;
    }
    messageHeader.attachForDecode(buffer, offset);
    final int messageLength = messageHeader.getMessageLength();

    if (messageLength <= 0 || (offset + messageLength > buffer.limit())) {
      return false;
    }

    ByteBuffer message = buffer.duplicate();
    message.order(buffer.order());
    message.position(offset);
    message.limit(offset + messageHeader.getMessageLength());

    action.accept(message);
    offset += messageLength;
    return true;
  }

  public boolean hasRemaining() {
    return offset < buffer.limit();
  }

  @Override
  public Spliterator<ByteBuffer> trySplit() {
    return null;
  }

}
