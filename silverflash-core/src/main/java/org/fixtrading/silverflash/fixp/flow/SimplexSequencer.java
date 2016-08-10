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

package org.fixtrading.silverflash.fixp.flow;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.fixtrading.silverflash.buffer.BufferArrays;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderEncoder;
import org.fixtrading.silverflash.fixp.messages.SequenceEncoder;
import org.fixtrading.silverflash.frame.MessageFrameEncoder;

/**
 * Applies Sequence message and updates sequence number for a simplex flow
 * <p>
 * A Sequence message is applied to each batch of application messages. This appropriate for a
 * datagram-oriented transport.
 * <p>
 * Not thread-safe; the caller is responsible for concurrency and FIFO message delivery.
 * 
 * @author Don Mendelson
 *
 */
public class SimplexSequencer implements Sequencer, MutableSequence {

  private final BufferArrays arrays = new BufferArrays();
  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private long nextSeqNo;
  private final ByteBuffer sequenceBuffer =
      ByteBuffer.allocateDirect(32).order(ByteOrder.nativeOrder());
  private final SequenceEncoder sequenceEncoder = new SequenceEncoder();
  private final MutableDirectBuffer directBuffer = new UnsafeBuffer(sequenceBuffer);
  private final MessageFrameEncoder frameEncoder;

  public SimplexSequencer(MessageFrameEncoder frameEncoder) {
    this(frameEncoder, 1);
  }

  public SimplexSequencer(MessageFrameEncoder frameEncoder, long nextSeqNo) {
    this.frameEncoder = frameEncoder;
    this.nextSeqNo = nextSeqNo;
    int offset = 0;
    frameEncoder.wrap(sequenceBuffer, offset).encodeFrameHeader();
    offset += frameEncoder.getHeaderLength();
    messageHeaderEncoder.wrap(directBuffer, offset);
    messageHeaderEncoder.blockLength(sequenceEncoder.sbeBlockLength())
        .templateId(sequenceEncoder.sbeTemplateId()).schemaId(sequenceEncoder.sbeSchemaId())
        .version(sequenceEncoder.sbeSchemaVersion());
    offset += messageHeaderEncoder.encodedLength();
    sequenceEncoder.wrap(directBuffer, offset);
    sequenceEncoder.nextSeqNo(nextSeqNo);
    frameEncoder
        .setMessageLength(messageHeaderEncoder.encodedLength() + sequenceEncoder.encodedLength())
        .encodeFrameTrailer();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.function.Function#apply(java.lang.Object)
   */
  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.fixp.flow.Sequencer#apply(java.nio.ByteBuffer[])
   */
  @Override
  public ByteBuffer[] apply(ByteBuffer[] buffers) {
    sequenceEncoder.nextSeqNo(nextSeqNo);
    ByteBuffer[] dest = arrays.getBufferArray(buffers.length + 1);
    dest[0] = sequenceBuffer;
    System.arraycopy(buffers, 0, dest, 1, buffers.length);
    nextSeqNo += buffers.length;
    return dest;
  }

  /**
   * @return the nextSeqNo
   */
  public long getNextSeqNo() {
    return nextSeqNo;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.fixp.flow.MutableSequence#setNextSeqNo(long)
   */
  public void setNextSeqNo(long nextSeqNo) {
    this.nextSeqNo = nextSeqNo;
  }

}
