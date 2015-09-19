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

package org.fixtrading.silverflash.fixp.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Returns a result set from
 * {@link MessageStore#retrieveMessagesAsync(MessageStoreResult, java.util.function.Consumer)}
 * 
 * @author Don Mendelson
 *
 */
public class MessageStoreResult {

  private static final int DEFAULT_CAPACITY = 128;

  private int countRequested;
  private Exception exception;
  private long fromSeqNo;
  private final ArrayList<ByteBuffer> messages = new ArrayList<>(DEFAULT_CAPACITY);
  private long requestTimestamp;
  private final UUID sessionId;
  private final AtomicBoolean isRetrieving = new AtomicBoolean();

  public MessageStoreResult(UUID sessionId) {
    this.sessionId = sessionId;
  }

  /**
   * Retrieve a range of messages from a message store
   * 
   * @param requestTimestamp time of request, used to match responses
   * @param fromSeqNo starting sequence number of the range
   * @param countRequested number of messages requested
   * @return Returns {@code true} if another request is not in progress
   */
  public boolean setRequest(long requestTimestamp, long fromSeqNo, int countRequested) {
    if (isRetrieving.compareAndSet(false, true)) {
      this.requestTimestamp = requestTimestamp;
      this.fromSeqNo = fromSeqNo;
      this.countRequested = countRequested;
      clearException();
      return true;
    } else {
      return false;
    }
  }

  /**
   * @return the countRequested
   */
  public int getCountRequested() {
    return countRequested;
  }

  /**
   * @return the exception
   */
  public Exception getException() {
    return exception;
  }

  public long getFromSeqNo() {
    return this.fromSeqNo;
  }

  public List<ByteBuffer> getMessages() {
    return Collections.unmodifiableList(messages);
  }

  public long getRequestTimestamp() {
    return this.requestTimestamp;
  }

  public UUID getSessionId() {
    return this.sessionId;
  }

  /**
   * Finished retrieving due to an exception.
   * 
   * @param e an asynchronous exception
   */
  public void setException(Exception e) {
    this.exception = e;
    isRetrieving.set(false);
  }

  /**
   * Finished retrieving successfully
   */
  public void finishedRetrieving() {
    isRetrieving.set(false);
  }

  public void clearException() {
    this.exception = null;
  }

  /**
   * Returns the requested range of messages as an immutable List
   * 
   * @param fromSeqNo starting sequence number of the range
   * @param count number of messages requested
   * @return a portion of the original request
   * @throws IndexOutOfBoundsException if sequence numbers are out of range
   */
  public List<ByteBuffer> getMessageList(long fromSeqNo, int count) {
    final int fromIndex = (int) (fromSeqNo - this.fromSeqNo);
    final int toIndex = fromIndex + count;

    if (toIndex > this.countRequested) {
      throw new IndexOutOfBoundsException(String.format("Count %s out of range", count));
    }

    // Immutable view of subrange - 'to' index is exclusive
    return Collections.unmodifiableList(messages.subList(fromIndex, toIndex));
  }

  public ByteBuffer getMessage(long seqNo) {
    final int index = (int) (seqNo - this.fromSeqNo);
    return messages.get(index);
  }

  public long getMessagesRemaining(long fromSeqNo) {
    return messages.size() - (fromSeqNo - this.fromSeqNo);
  }

  /**
   * Returns a mutable List to populate
   * 
   * @return list of messages
   */
  ArrayList<ByteBuffer> getMessageList() {
    return messages;
  }

  public boolean isRangeContained(long fromSeqNo, int count) {
    return fromSeqNo >= this.fromSeqNo && fromSeqNo + count <= this.fromSeqNo + this.countRequested;
  }
}
