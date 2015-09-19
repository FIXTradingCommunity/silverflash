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

package org.fixtrading.silverflash.buffer;

import java.nio.ByteBuffer;

/**
 * Preallocated buffer arrays of various lengths
 * 
 * This implementation supplies buffer arrays up to length 32.
 * 
 * @author Don Mendelson
 *
 */
public class BufferArrays {

  private final ByteBuffer[][] arrays = new ByteBuffer[32][];

  /**
   * Allocate buffer arrays
   */
  public BufferArrays() {
    for (int length = 0; length < arrays.length; length++) {
      arrays[length] = new ByteBuffer[length];
    }
  }


  /**
   * Supply a buffer array of the specified length
   * 
   * @param length number of buffers
   * @return a buffer array
   */
  public ByteBuffer[] getBufferArray(int length) {
    return arrays[length];
  }
}
