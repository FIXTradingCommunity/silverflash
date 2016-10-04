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

package io.fixprotocol.silverflash.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import io.fixprotocol.silverflash.buffer.BufferSupplier;

/**
 * A channel to send and receive messages (OSI layer 4)
 * 
 * @author Don Mendelson
 *
 */
public interface Transport {

  /**
   * Open this Transport asynchronously
   * 
   * @param buffers Supplier of buffers to hold received messages
   * @param consumer message receiver and event handler
   * @return a future that triggers action when successfully completed or when an exception 
   * occurs. Possible exceptions include IOException.
   */
  CompletableFuture<? extends Transport> open(BufferSupplier buffers, TransportConsumer consumer);

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
   * Does this Transport recognize message boundaries?
   * 
   * @return Returns {@code true} if each call to {@link #read} returns a single message
   * or {@code false} if it returns a stream containing any number of messages or
   * partial messages
   */
  boolean isMessageOriented();

  /**
   * Tells whether this Transport is open
   * 
   * @return Returns {@code true} if this Transport is open
   */
  boolean isOpen();
  
  /**
   * Tells whether this Transport is ready to read
   * 
   * @return Returns {@code true} if this Transport is ready to read
   */
  boolean isReadyToRead();
}
