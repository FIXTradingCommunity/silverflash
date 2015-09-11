package org.fixtrading.silverflash.fixp.flow;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.fixtrading.silverflash.buffer.BufferArrays;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageType;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.SequenceEncoder;

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

  private long nextSeqNo;
  private final ByteBuffer sequenceBuffer = ByteBuffer.allocateDirect(18).order(
      ByteOrder.nativeOrder());
  private final MessageEncoder messageEncoder = new MessageEncoder();
  private final SequenceEncoder sequenceEncoder;
  private final BufferArrays arrays = new BufferArrays();
  private boolean firstTime = true;

  public SimplexStreamSequencer() {
    this(1);
  }

  public SimplexStreamSequencer(long nextSeqNo) {
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
    if (firstTime) {
      sequenceEncoder.setNextSeqNo(nextSeqNo);
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
   * @see org.fixtrading.silverflash.fixp.flow.MutableSequence#setNextSeqNo(long)
   */
  public void setNextSeqNo(long nextSeqNo) {
    this.nextSeqNo = nextSeqNo;
  }

}
