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
   * Copies payload to a destination buffer
   */
  public void setPayload(ByteBuffer src, ByteBuffer dest) {
    dest.clear();
    if (src != null) {
      src.flip();
      dest.put(src);
    }
  }

}
