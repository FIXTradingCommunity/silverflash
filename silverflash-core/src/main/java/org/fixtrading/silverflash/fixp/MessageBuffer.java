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

package org.fixtrading.silverflash.fixp;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.fixtrading.silverflash.MessageConsumer;
import org.fixtrading.silverflash.Session;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * A buffer for asynchronous processing of received messages
 * 
 * @author Don Mendelson
 *
 */
public class MessageBuffer implements MessageConsumer<UUID> {

  private static class BufferEvent {
    static final EventFactory<BufferEvent> EVENT_FACTORY = BufferEvent::new;
    private static int capacity = 2048;
    private final ByteBuffer buffer;
    private long seqNo;
    private Session<UUID> session;

    BufferEvent() {
      buffer = ByteBuffer.allocateDirect(capacity).order(ByteOrder.nativeOrder());
    }

    ByteBuffer getBuffer() {
      return buffer;
    }

    /**
     * @return the seqNo
     */
    long getSeqNo() {
      return seqNo;
    }

    /**
     * @return the fixpSession
     */
    Session<UUID> getSession() {
      return session;
    }

    void set(Session<UUID> session, ByteBuffer message, long seqNo) {
      this.session = session;
      this.seqNo = seqNo;
      buffer.clear();
      if (message != null) {
        buffer.put(message);
        buffer.flip();
      }
    }
  }

  private Disruptor<BufferEvent> disruptor;
  private final AtomicBoolean isRunning = new AtomicBoolean();
  private final MessageConsumer<UUID> receiver;
  private RingBuffer<BufferEvent> ringBuffer;
  private final int ringSize = 256;
  private final ThreadFactory threadFactory;

  /**
   * @param threadFactory executes events
   * @param receiver receives messages asynchronously
   */
  public MessageBuffer(ThreadFactory threadFactory, MessageConsumer<UUID> receiver) {
    this.threadFactory = threadFactory;
    this.receiver = receiver;
  }

  public void accept(ByteBuffer message, Session<UUID> session, long seqNo) {
    offer(session, message, seqNo);

  }

  public void shutdown() {
    if (isRunning.compareAndSet(true, false)) {
      this.disruptor.halt();
    }
  }

  @SuppressWarnings("unchecked")
  public void start() {
    if (isRunning.compareAndSet(false, true)) {
      this.disruptor =
          new Disruptor<>(BufferEvent.EVENT_FACTORY, ringSize, threadFactory, ProducerType.SINGLE,
              new BusySpinWaitStrategy());
      this.disruptor.handleEventsWith(this::handleEvent);
      this.ringBuffer = disruptor.start();
    }
  }

  void offer(Session<UUID> session, ByteBuffer src, long seqNo) {
    long sequence = ringBuffer.next();
    final BufferEvent event = ringBuffer.get(sequence);
    event.set(session, src, seqNo);
    ringBuffer.publish(sequence);
  }

  private void handleEvent(BufferEvent event, long sequence, boolean endOfBatch) {
    receiver.accept(event.getBuffer(), event.getSession(), event.getSeqNo());
  }


}
