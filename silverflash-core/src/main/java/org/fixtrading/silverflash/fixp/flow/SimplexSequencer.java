/**
 *    Copyright 2015 FIX Protocol Ltd
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

package org.fixtrading.silverflash.fixp.flow;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.fixtrading.silverflash.buffer.BufferArrays;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageType;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.SequenceEncoder;

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

  private long nextSeqNo;
  private final ByteBuffer sequenceBuffer = ByteBuffer.allocateDirect(18).order(
      ByteOrder.nativeOrder());
  private final MessageEncoder messageEncoder = new MessageEncoder();
  private final SequenceEncoder sequenceEncoder;
  private final BufferArrays arrays = new BufferArrays();

  public SimplexSequencer() {
    this(1);
  }

  public SimplexSequencer(long nextSeqNo) {
    this.nextSeqNo = nextSeqNo;
    sequenceEncoder =
        (SequenceEncoder) messageEncoder.attachForEncode(sequenceBuffer, 0, MessageType.SEQUENCE);
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
    sequenceEncoder.setNextSeqNo(nextSeqNo);
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
