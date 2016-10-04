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
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderEncoder;
import io.fixprotocol.silverflash.fixp.messages.SequenceEncoder;
import io.fixprotocol.silverflash.frame.MessageFrameEncoder;
import uk.co.real_logic.sbe.ir.generated.MessageHeaderDecoder;

/**
 * Applies Sequence message and updates sequence number for a simplex flow on a stream Transport
 * <p>
 * A Sequence message is applied only at the begging of a flow.
 * <p>
 * Not thread-safe; the caller is responsible for concurrency and FIFO message delivery.
 * 
 * @author Don Mendelson
 *
 */
public class SimplexStreamSequencer implements Sequencer, MutableSequence {

  private final BufferArrays arrays = new BufferArrays();
  private boolean firstTime = true;
  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private long nextSeqNo;
  private final ByteBuffer sequenceBuffer =
      ByteBuffer.allocateDirect(32).order(ByteOrder.nativeOrder());
  private final SequenceEncoder sequenceEncoder = new SequenceEncoder();
  private final MutableDirectBuffer directBuffer = new UnsafeBuffer(sequenceBuffer);
  private final MessageFrameEncoder frameEncoder;

  public SimplexStreamSequencer(MessageFrameEncoder frameEncoder) {
    this(frameEncoder, 1);
  }

  public SimplexStreamSequencer(MessageFrameEncoder frameEncoder, long nextSeqNo) {
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
    frameEncoder.setMessageLength(offset + sequenceEncoder.encodedLength());
    frameEncoder.encodeFrameTrailer();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.function.Function#apply(java.lang.Object)
   */
  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.fixp.flow.Sequencer#apply(java.nio.ByteBuffer[])
   */
  @Override
  public ByteBuffer[] apply(ByteBuffer[] buffers) {
    if (firstTime) {
      sequenceEncoder.nextSeqNo(nextSeqNo);
      ByteBuffer[] dest = arrays.getBufferArray(buffers.length + 1);
      dest[0] = sequenceBuffer;
      System.arraycopy(buffers, 0, dest, 1, buffers.length);
      nextSeqNo += buffers.length;
      firstTime = false;
      return dest;
    } else {
      nextSeqNo += buffers.length;
      return buffers;
    }
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
   * @see io.fixprotocol.silverflash.fixp.flow.MutableSequence#setNextSeqNo(long)
   */
  public void setNextSeqNo(long nextSeqNo) {
    this.nextSeqNo = nextSeqNo;
  }

}
