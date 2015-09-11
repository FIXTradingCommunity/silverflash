package org.fixtrading.silverflash;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Message sender
 * 
 * @author Don Mendelson
 *
 */
@FunctionalInterface
public interface Sender {

  /**
   * Sends a message on a stream.
   * <p>
   * The message is opaque to the session protocol; it is treated as an arbitrary sequence of bytes.
   * 
   * @param message a buffer containing a message to send. Expectation of the buffer position on
   *        entry is the same as for {@link java.nio.channels.WritableByteChannel#write}.
   * @return sequence number of a successfully sent message or zero if the stream is unsequenced
   * @throws IOException if an IO error occurs
   * @see java.nio.channels.WriteableByteChannel#write
   * 
   */
  long send(ByteBuffer message) throws IOException;

  /**
   * Sends a batch of messages on a stream
   * 
   * @param messages a batch of message buffers. See {@link #send(ByteBuffer)} for the expectation
   *        for each buffer.
   * @return sequence number of last message successfully sent or zero if the stream is unsequenced
   * @throws IOException if an IO error occurs
   */
  default long send(ByteBuffer[] messages) throws IOException {
    long seqNo = 0;
    for (int i = 0; i < messages.length; i++) {
      seqNo = send(messages[i]);
    }
    return seqNo;
  }
}
