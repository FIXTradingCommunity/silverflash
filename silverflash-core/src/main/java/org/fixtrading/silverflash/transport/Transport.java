package org.fixtrading.silverflash.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 * A channel to send and receive messages (OSI layer 4)
 * 
 * @author Don Mendelson
 *
 */
public interface Transport {

  /**
   * Open this Transport
   * 
   * @param buffers Supplier of buffers to hold received messages
   * @param consumer message receiver and event handler
   * @throws IOException if the Transport cannot be opened
   */
  void open(Supplier<ByteBuffer> buffers, TransportConsumer consumer) throws IOException;

  /**
   * Close this Transport
   */
  void close();

  /**
   * Read received data into a buffer
   * 
   * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream
   * @throws IOException if an IO error occurs
   * @see java.nio.channels.ReadableByteChannel#read(ByteBuffer)
   */
  int read() throws IOException;

  /**
   * Writes contents of a message buffer
   * 
   * @param src buffer containing bytes to write
   * @return The number of bytes written, possibly zero
   * @throws IOException if an IO error occurs
   * @see java.nio.channels.WritableByteChannel#write(ByteBuffer)
   */
  int write(ByteBuffer src) throws IOException;

  /**
   * Writes contents of message buffers
   * 
   * @param srcs buffers containing bytes to write
   * @return The number of bytes written, possibly zero
   * @throws IOException if an IO error occurs
   * @see java.nio.channels.GatheringByteChannel#write(ByteBuffer[])
   */
  default long write(ByteBuffer[] srcs) throws IOException {
    long bytesWritten = 0;
    for (int i = 0; i < srcs.length; i++) {
      bytesWritten += write(srcs[i]);
    }
    return bytesWritten;
  }

  /**
   * Does this Transport guarantee in-order delivery of messages?
   * 
   * @return Returns {@code true} if this Transport enforces delivery order
   */
  boolean isFifo();

  /**
   * Tells whether this Transport is open
   * 
   * @return Returns {@code true} if this Transport is open
   */
  boolean isOpen();
}
