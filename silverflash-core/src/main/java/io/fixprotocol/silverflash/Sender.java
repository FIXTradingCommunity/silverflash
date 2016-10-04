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

package io.fixprotocol.silverflash;

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
   *        entry is the same as for {@code java.nio.channels.WritableByteChannel.write()}.
   * @return sequence number of a successfully sent message or zero if the stream is unsequenced
   * @throws IOException if an IO error occurs
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
