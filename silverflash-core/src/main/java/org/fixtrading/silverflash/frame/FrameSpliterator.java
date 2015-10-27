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
import java.util.Spliterator;

/**
 * Delimits complete message frames in a buffer
 * 
 * @author Don Mendelson
 *
 */
public interface FrameSpliterator extends Spliterator<ByteBuffer> {

  /**
   * Returns whether a buffer contains any remaining full or partial frames
   * 
   * @return Returns {@code true} if a full frame or partial message remains in the buffer
   */
  boolean hasRemaining();

  /**
   * Set buffer to delimit
   * 
   * @param buffer
   *          a buffer containing message frames
   */
  void wrap(ByteBuffer buffer);

}
