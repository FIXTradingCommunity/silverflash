package org.fixtrading.silverflash.buffer;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 * A Supplier of message buffer that uses a single, fixed ByteBuffer
 * 
 * @author Don Mendelson
 *
 */
public class SingleBufferSupplier implements Supplier<ByteBuffer> {

  private final ByteBuffer buffer;

  /**
   * Constructor
   * 
   * @param buffer an allocated buffer
   */
  public SingleBufferSupplier(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public ByteBuffer get() {
    return buffer;
  }

}
