package org.fixtrading.silverflash.fixp.flow;

import org.fixtrading.silverflash.Sequenced;

/**
 * A mutable sequence
 * 
 * @author Don Mendelson
 *
 */
public interface MutableSequence extends Sequenced {

  /**
   * Set the next expected sequence number
   * 
   * @param nextSeqNo a sequence number
   */
  void setNextSeqNo(long nextSeqNo);
}
