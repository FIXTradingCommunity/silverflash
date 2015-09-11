package org.fixtrading.silverflash.transport;

/**
 * Transport events
 * 
 * @author Don Mendelson
 *
 */
public interface Channel {

  /**
   * Invoked when a Channel is ready to read
   */
  void readyToRead();

  /**
   * Invoked when a Channel is ready to write
   */
  void readyToWrite();
}
