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

package io.fixprotocol.silverflash.fixp.flow;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import io.fixprotocol.silverflash.buffer.BufferArrays;
import io.fixprotocol.silverflash.fixp.messages.ContextEncoder;
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderEncoder;
import io.fixprotocol.silverflash.frame.MessageFrameEncoder;

/**
 * Applies Context message and updates sequence number for a multiplexed flow
 * <p>
 * Not thread-safe; the caller is responsible for concurrency and FIFO message delivery.
 * 
 * @author Don Mendelson
 *
 */
public class MultiplexSequencer implements Sequencer, MutableSequence {

  private final BufferArrays arrays = new BufferArrays();
  private final ByteBuffer contextBuffer =
      ByteBuffer.allocateDirect(48).order(ByteOrder.nativeOrder());
  private final ContextEncoder contextEncoder = new ContextEncoder();
  private final MutableDirectBuffer directBuffer = new UnsafeBuffer(contextBuffer);
  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private long nextSeqNo;
  private final MessageFrameEncoder frameEncoder;

  public MultiplexSequencer(MessageFrameEncoder frameEncoder, byte[] uuidAsBytes) {
    this(frameEncoder, uuidAsBytes, 1);
  }

  public MultiplexSequencer(MessageFrameEncoder frameEncoder, byte[] uuidAsBytes, long nextSeqNo) {
    this.frameEncoder = frameEncoder;
    this.nextSeqNo = nextSeqNo;
    int offset = 0;
    frameEncoder.wrap(contextBuffer, offset).encodeFrameHeader();
    offset += frameEncoder.getHeaderLength();
    messageHeaderEncoder.wrap(directBuffer, offset);
    messageHeaderEncoder.blockLength(contextEncoder.sbeBlockLength())
        .templateId(contextEncoder.sbeTemplateId()).schemaId(contextEncoder.sbeSchemaId())
        .version(contextEncoder.sbeSchemaVersion());
    offset += messageHeaderEncoder.encodedLength();
    contextEncoder.wrap(directBuffer, offset);
    for (int i = 0; i < 16; i++) {
      contextEncoder.sessionId(i, uuidAsBytes[i]);
    }
    contextEncoder.nextSeqNo(nextSeqNo);
    frameEncoder.setMessageLength(offset + contextEncoder.encodedLength());
    frameEncoder.encodeFrameTrailer();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.function.Function#apply(java.lang.Object)
   */
  public ByteBuffer[] apply(ByteBuffer[] buffers) {
    contextEncoder.nextSeqNo(nextSeqNo);
    ByteBuffer[] dest = arrays.getBufferArray(buffers.length + 1);
    dest[0] = contextBuffer;
    System.arraycopy(buffers, 0, dest, 1, buffers.length);
    nextSeqNo += buffers.length;
    return dest;
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.Sequenced#getNextSeqNo()
   */
  public long getNextSeqNo() {
    return nextSeqNo;
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.fixp.flow.MutableSequence#setNextSeqNo(long)
   */
  public void setNextSeqNo(long nextSeqNo) {
    this.nextSeqNo = nextSeqNo;
  }
}
