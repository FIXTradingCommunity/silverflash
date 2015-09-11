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
  private final ByteBuffer contextBuffer = ByteBuffer.allocateDirect(34).order(
      ByteOrder.nativeOrder());
  private final MessageEncoder messageEncoder = new MessageEncoder();
  private final ContextEncoder contextEncoder;
  private final BufferArrays arrays = new BufferArrays();

  public MultiplexSequencer(byte[] uuidAsBytes) {
    this(uuidAsBytes, 1);
  }

  public MultiplexSequencer(byte[] uuidAsBytes, long nextSeqNo) {
    this.nextSeqNo = nextSeqNo;
    contextEncoder =
        (ContextEncoder) messageEncoder.attachForEncode(contextBuffer, 0, MessageType.CONTEXT);
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
