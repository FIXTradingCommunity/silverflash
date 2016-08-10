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

package org.fixtrading.silverflash;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Retransmits messages on a recoverable flow
 * 
 * @author Don Mendelson
 *
 */
public interface RecoverableSender extends Sender {

  /**
   * Resends a sequenced message on a stream.
   * <p>
   * The message is opaque to the session protocol; it is treated as an arbitrary sequence of bytes.
   * 
   * @param message a buffer containing a message to send. Expectation of the buffer position on
   *        entry is the same as for {@code java.nio.channels.WritableByteChannel.write()}.
   * @param seqNo sequence number
   * @param requestTimestamp time that this retransmission was requested
   * @throws IOException if an IO error occurs
   */
  void resend(ByteBuffer message, long seqNo, long requestTimestamp) throws IOException;

  /**
   * Resends a batch of messages on a stream
   * 
   * @param messages a batch of message buffers. See {@link #resend(ByteBuffer, long, long)} for the
   *        expectation for each buffer.
   * @param offset The offset within the message array of the first message to be sent; must be
   *        non-negative and no larger than messages.length
   * @param length The maximum number of messages to be sent; must be non-negative and no larger
   *        than messages.length - offset
   * @param seqNo sequence number of the first message in the batch
   * @param requestTimestamp time that this retransmission was requested
   * @throws IOException if an IO error occurs
   */
  default void resend(ByteBuffer[] messages, int offset, int length, long seqNo,
      long requestTimestamp) throws IOException {
    for (int i = offset; i < offset + length; i++) {
      resend(messages[i], seqNo + 1, requestTimestamp);
    }
  }
}
