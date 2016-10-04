/**
 *    Copyright 2015-2016 FIX Protocol Ltd
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
package io.fixprotocol.silverflash.frame.sofh;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

import io.fixprotocol.silverflash.frame.FrameSpliterator;

/**
 * @author Don Mendelson
 *
 */
public class SofhFrameSpliterator implements FrameSpliterator {

  private ByteBuffer buffer;
  private final SofhFrameDecoder decoder = new SofhFrameDecoder();
  private int offset;
  private ByteOrder originalByteOrder;

  /**
   * Constructor
   */
  public SofhFrameSpliterator() {

  }

  /**
   * Construct an instance and wrap a buffer
   * @param buffer buffer containing messages to delimit
   */
  public SofhFrameSpliterator(ByteBuffer buffer) {
    wrap(buffer);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.Spliterator#characteristics()
   */
  @Override
  public int characteristics() {
    return DISTINCT | NONNULL | IMMUTABLE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.Spliterator#estimateSize()
   */
  @Override
  public long estimateSize() {
    return buffer.hasRemaining() ? Long.MAX_VALUE : 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.buffer.FrameSpliterator#hasRemaining()
   */
  @Override
  public boolean hasRemaining() {
    return buffer.remaining() > 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.Spliterator#tryAdvance(java.util.function.Consumer)
   */
  @Override
  public boolean tryAdvance(Consumer<? super ByteBuffer> action) {
    Objects.requireNonNull(action);
    int messageOffset = offset + SofhFrameDecoder.HEADER_LENGTH;
    if (messageOffset > buffer.limit()) {
      return false;
    }
    decoder.wrap(buffer);
    decoder.decodeFrameHeader();
    final int messageLength = decoder.getMessageLength();
    int messageLimit = offset + SofhFrameDecoder.HEADER_LENGTH + messageLength;

    if (messageLength <= 0 || (messageLimit > buffer.limit())) {
      return false;
    }

    ByteBuffer message = buffer.duplicate();
    message.order(originalByteOrder);
    message.limit(messageLimit);

    action.accept(message);
    buffer.position(messageLimit);
    // offset is set to same value as position, but can't depend on position not being altered by
    // app
    offset = messageLimit;
    buffer.order(originalByteOrder);
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.Spliterator#trySplit()
   */
  @Override
  public Spliterator<ByteBuffer> trySplit() {
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.buffer.FrameSpliterator#wrap(java.nio.ByteBuffer)
   */
  @Override
  public void wrap(ByteBuffer buffer) {
    Objects.requireNonNull(buffer);
    this.buffer = buffer;
    this.offset = buffer.position();
    this.originalByteOrder = buffer.order();
  }

}
