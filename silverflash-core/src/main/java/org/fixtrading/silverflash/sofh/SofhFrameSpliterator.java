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
package org.fixtrading.silverflash.sofh;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

import org.fixtrading.silverflash.buffer.FrameSpliterator;

/**
 * @author Donald
 *
 */
public class SofhFrameSpliterator implements FrameSpliterator {

  private ByteBuffer buffer;
  private ByteOrder originalByteOrder;
  private SofhFrameDecoder decoder = new SofhFrameDecoder();

  public SofhFrameSpliterator() {
    
  }
  /**
   * @param buffer2
   */
  public SofhFrameSpliterator(ByteBuffer buffer) {
    wrap(buffer);
  }

  /* (non-Javadoc)
   * @see java.util.Spliterator#tryAdvance(java.util.function.Consumer)
   */
  @Override
  public boolean tryAdvance(Consumer<? super ByteBuffer> action) {
    Objects.requireNonNull(action);
    int offset = buffer.position();

    int messageOffset = offset + SofhFrameDecoder.HEADER_LENGTH;
    if (messageOffset > buffer.limit()) {
      return false;
    }
    decoder.wrap(buffer);
    decoder.decodeFrameHeader();
    final int messageLength = decoder.getMessageLength();

    if (messageLength <= 0 || (offset + messageLength > buffer.limit())) {
      return false;
    }

    ByteBuffer message = buffer.duplicate();
    message.order(originalByteOrder);
    message.limit(offset + SofhFrameDecoder.HEADER_LENGTH + messageLength);

    action.accept(message);
    offset += (SofhFrameDecoder.HEADER_LENGTH + messageLength);
    buffer.position(offset);
    buffer.order(originalByteOrder);
    return true;
  }

  /* (non-Javadoc)
   * @see java.util.Spliterator#trySplit()
   */
  @Override
  public Spliterator<ByteBuffer> trySplit() {
    return null;
  }

  /* (non-Javadoc)
   * @see java.util.Spliterator#estimateSize()
   */
  @Override
  public long estimateSize() {
    return buffer.hasRemaining() ? Long.MAX_VALUE : 0;
  }

  /* (non-Javadoc)
   * @see java.util.Spliterator#characteristics()
   */
  @Override
  public int characteristics() {
    return DISTINCT | NONNULL | IMMUTABLE;
  }

  /* (non-Javadoc)
   * @see org.fixtrading.silverflash.buffer.FrameSpliterator#wrap(java.nio.ByteBuffer)
   */
  @Override
  public void wrap(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    this.buffer = buffer;
    this.originalByteOrder = buffer.order();
  }

  /* (non-Javadoc)
   * @see org.fixtrading.silverflash.buffer.FrameSpliterator#hasRemaining()
   */
  @Override
  public boolean hasRemaining() {
    return buffer.remaining() > 0;
  }

}
