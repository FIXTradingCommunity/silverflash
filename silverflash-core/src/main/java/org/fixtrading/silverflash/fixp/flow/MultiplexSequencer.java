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
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.ContextEncoder;

/**
 * Applies Context message and updates sequence number for a multiplexed flow
 * <p>
 * Not thread-safe; the caller is responsible for concurrency and FIFO message delivery.
 * 
 * @author Don Mendelson
 *
 */
public class MultiplexSequencer implements Sequencer, MutableSequence {

  private long nextSeqNo;
  private final ByteBuffer contextBuffer = ByteBuffer.allocateDirect(48).order(
      ByteOrder.nativeOrder());
  private final ContextEncoder contextEncoder;
  private final BufferArrays arrays = new BufferArrays();

  public MultiplexSequencer(byte[] uuidAsBytes, MessageEncoder messageEncoder) {
    this(uuidAsBytes, 1, messageEncoder);
  }

  public MultiplexSequencer(byte[] uuidAsBytes, long nextSeqNo, MessageEncoder messageEncoder) {
    this.nextSeqNo = nextSeqNo;
    contextEncoder =
        (ContextEncoder) messageEncoder.wrap(contextBuffer, 0, MessageType.CONTEXT);
    contextEncoder.setSessionId(uuidAsBytes);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.function.Function#apply(java.lang.Object)
   */
  public ByteBuffer[] apply(ByteBuffer[] buffers) {
    contextEncoder.setNextSeqNo(nextSeqNo);
    ByteBuffer[] dest = arrays.getBufferArray(buffers.length + 1);
    dest[0] = contextBuffer;
    System.arraycopy(buffers, 0, dest, 1, buffers.length);
    nextSeqNo += buffers.length;
    return dest;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.Sequenced#getNextSeqNo()
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
