package org.fixtrading.silverflash.fixp.store;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Consumer;

import org.fixtrading.silverflash.Service;

/**
 * Stores and retrieves messages
 * <p>
 * Only outbound recoverable messages are stored, so no distinction is made between inbound and
 * outbound sequences.
 * 
 * @author Don Mendelson
 *
 */
public interface MessageStore extends Service {

  /**
   * Insert a message
   * 
   * @param sessionId session identifier
   * @param seqNo sequence number of the message
   * @param message message to insert
   * @throws StoreException if the message cannot be inserted into the store
   */
  void insertMessage(UUID sessionId, long seqNo, ByteBuffer message) throws StoreException;

  /**
   * Request a range of messages. The result is returned to a consumer asynchronously.
   * 
   * @param result result to populate
   * @param consumer handler of populated result
   * @throws StoreException if the message query cannot be executed
   */
  void retrieveMessagesAsync(MessageStoreResult result, Consumer<MessageStoreResult> consumer)
      throws StoreException;

  /**
   * Retrieve the maximum sequence number stored for a session
   * 
   * @param sessionId session identifier
   * @return sequence number, or zero if no messages have been stored for the session
   */
  long retrieveMaxSeqNo(UUID sessionId);

}
