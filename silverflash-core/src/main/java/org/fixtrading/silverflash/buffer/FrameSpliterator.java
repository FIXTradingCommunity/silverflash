package org.fixtrading.silverflash.buffer;

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
   * Set buffer to delimit
   * 
   * @param buffer a buffer containing message frames
   */
  void wrap(ByteBuffer buffer);

  /**
   * Returns whether a buffer contains any remaining full or partial frames
   * 
   * @return Returns {@code true} if a full frame or partial message remains in the buffer
   */
  boolean hasRemaining();


}
