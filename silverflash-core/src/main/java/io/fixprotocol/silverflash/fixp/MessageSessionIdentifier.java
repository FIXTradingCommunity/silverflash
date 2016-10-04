/**
 * Copyright 2015-2016 FIX Protocol Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.fixprotocol.silverflash.fixp;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import io.fixprotocol.silverflash.fixp.SessionId;
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderDecoder;

/**
 * Retrieves session ID from a FIXP session message
 * 
 * Optimized to take advantage of change in FIXP RC3 to always put session ID in same position in
 * session messages.
 * <p>
 * Assumes that message frames have already been delimited.
 * 
 * @author Don Mendelson
 *
 */
public class MessageSessionIdentifier implements Function<ByteBuffer, UUID> {

  public static final int SCHEMA_ID = 2748;

  private final DirectBuffer immutableBuffer = new UnsafeBuffer(new byte[0]);
  private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
  private final byte[] sessionId = new byte[16];

  /*
   * (non-Javadoc)
   * 
   * @see java.util.function.Function#apply(java.lang.Object)
   */
  public UUID apply(ByteBuffer buffer) {
    immutableBuffer.wrap(buffer);
    int offset = buffer.position();
    messageHeaderDecoder.wrap(immutableBuffer, offset);
    if (messageHeaderDecoder.schemaId() == SCHEMA_ID) {
      offset += messageHeaderDecoder.encodedLength();

      for (int i = 0; i < 16; i++) {
        sessionId[i] = immutableBuffer.getByte(offset + i);
      }

      return SessionId.UUIDFromBytes(sessionId);
    } else {
      return null;
    }
  }

}
