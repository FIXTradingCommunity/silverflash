package org.fixtrading.silverflash;

/**
 * Events ordered by a sequence number
 * 
 * @author Don Mendelson
 *
 */
@FunctionalInterface
public interface Sequenced {

  /**
   * Returns the next expected sequence number
   * 
   * @return next sequence number
   */
  long getNextSeqNo();

}
