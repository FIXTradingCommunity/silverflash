package org.fixtrading.silverflash;

import java.nio.ByteBuffer;



/**
 * Consumes messages received on a Session (application layer)
 * 
 * @author Don Mendelson
 * 
 * @param T session identifier type
 */
@FunctionalInterface
public interface MessageConsumer<T> {

  /**
   * Consume a message
   * 
   * @param message buffer holding a message
   * @param session session that delivered a message
   * @param seqNo sequence number or zero if the flow is unsequenced
   */
  void accept(ByteBuffer message, Session<T> session, long seqNo);
}
