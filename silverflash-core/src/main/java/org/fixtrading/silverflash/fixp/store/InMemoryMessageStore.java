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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * In-memory MessageStore implementation suitable for functional testing Does not persist data.
 * 
 * @author Don Mendelson
 *
 */
public class InMemoryMessageStore implements MessageStore {

  private static final ByteBuffer PLACEHOLDER = ByteBuffer.allocate(0);
  private final Map<UUID, ArrayList<ByteBuffer>> messageMap = new ConcurrentHashMap<>();



  /*
   * (non-Javadoc)
   * 
   * @see java.lang.AutoCloseable#close()
   */
  public void close() throws Exception {
    messageMap.clear();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.fixp.store.MessageStore#open()
   */
  public CompletableFuture<InMemoryMessageStore> open() {
    return CompletableFuture.completedFuture(this);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.fixp.store.MessageStore#insertMessage(java.util.UUID, long,
   * java.nio.ByteBuffer)
   */
  public void insertMessage(UUID sessionId, long seqNo, ByteBuffer message) throws StoreException {
    ArrayList<ByteBuffer> messages = messageMap.get(sessionId);
    if (messages == null) {
      messages = new ArrayList<>();
      messageMap.put(sessionId, messages);
    }
    for (int i = messages.size(); i < seqNo; i++) {
      messages.add(PLACEHOLDER);
    }
    messages.set((int) (seqNo - 1), message);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.fixtrading.silverflash.fixp.store.MessageStore#retrieveMessagesAsync(org.fixtrading.silverflash
   * .fixp.store.MessageStoreResult, java.util.function.Consumer)
   */
  public void retrieveMessagesAsync(MessageStoreResult result, Consumer<MessageStoreResult> consumer)
      throws StoreException {
    final UUID sessionId = result.getSessionId();
    final ArrayList<ByteBuffer> messages = messageMap.get(sessionId);
    if (messages != null) {
      int offset = (int) (result.getFromSeqNo() - 1);
      int count = Math.min(result.getCountRequested(), messages.size() - offset);

      ArrayList<ByteBuffer> arrayList = result.getMessageList();
      arrayList.clear();
      arrayList.addAll(messages.subList(offset, offset + count));

      // synchronous return
      consumer.accept(result);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.fixp.store.MessageStore#retrieveMaxSeqNo(java.util.UUID)
   */
  public long retrieveMaxSeqNo(UUID sessionId) {
    final ArrayList<ByteBuffer> messages = messageMap.get(sessionId);
    if (messages != null) {
      return messages.size();
    } else {
      return 0;
    }
  }

}
