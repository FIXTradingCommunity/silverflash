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

package org.fixtrading.silverflash.reactor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Allocates ByteBuffer
 * 
 * @author Don Mendelson
 *
 */
public class ByteBufferPayload implements PayloadAllocator<ByteBuffer> {

  private final int capacity;

  /**
   * Constructor
   * 
   * @param capacity size of ByteBuffer to be allocated
   */
  public ByteBufferPayload(int capacity) {
    this.capacity = capacity;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.reactor.Payload#allocatePayload()
   */
  public ByteBuffer allocatePayload() {
    return ByteBuffer.allocateDirect(capacity).order(ByteOrder.nativeOrder());
  }

  /**
   * Copies payload to a destination buffer. 
   * @param src source buffer
   * @param dest destination buffer
   */
  public void setPayload(ByteBuffer src, ByteBuffer dest) {
    dest.clear();
    if (src != null) {
      src.flip();
      dest.put(src);
    }
  }

}
