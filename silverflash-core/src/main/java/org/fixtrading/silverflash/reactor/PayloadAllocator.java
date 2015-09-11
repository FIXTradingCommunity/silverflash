package org.fixtrading.silverflash.reactor;

/**
 * Allocates payload for an event
 * 
 * @author Don Mendelson
 *
 */
public interface PayloadAllocator<T> {

  /**
   * Allocates a paylod
   * 
   * @return a newly allocated payload
   */
  T allocatePayload();

  /**
   * Copies payload to a destination
   * 
   * @param src source payload
   * @param dest destination
   */
  void setPayload(T src, T dest);
}
