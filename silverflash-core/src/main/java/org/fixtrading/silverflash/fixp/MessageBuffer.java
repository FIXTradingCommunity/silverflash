package org.fixtrading.silverflash.fixp;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import java.util.concurrent.Executor;
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
  private final Executor executor;
  private final AtomicBoolean isRunning = new AtomicBoolean();
  private final MessageConsumer<UUID> receiver;
  private RingBuffer<BufferEvent> ringBuffer;
  private final int ringSize = 256;

  /**
   * @param executor executes events
   * @param receiver receives messages asynchronously
   */
  public MessageBuffer(Executor executor, MessageConsumer<UUID> receiver) {
    this.executor = executor;
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
          new Disruptor<>(BufferEvent.EVENT_FACTORY, ringSize, executor, ProducerType.SINGLE,
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
