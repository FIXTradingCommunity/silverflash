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
