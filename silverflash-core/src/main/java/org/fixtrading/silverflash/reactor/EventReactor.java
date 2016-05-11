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

package org.fixtrading.silverflash.reactor;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.fixtrading.silverflash.ExceptionConsumer;
import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.Service;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Subscription and publication of message events by Topic.
 * <p>
 * When an event is dispatched, a message is sent to a subscribing Receiver. Threading policy is
 * supplied externally to this class by an Executor. Dispatching may be single or multi-threaded
 * since the registry of topics is thread-safe. Receiver implementations are responsible for their
 * own thread safety if multi-threaded dispatching is used.
 * <p>
 * This implementation unicasts events. That is, there can be at most one subscriber per Topic.
 * Wildcard topics are not supported.
 * <p>
 * 
 * @author Don Mendelson
 */
public class EventReactor<T> implements Service {

  private class BufferEvent {

    private final T payload;
    private Topic topic;

    BufferEvent() {
      payload = payloadAllocator.allocatePayload();
    }

    T getPayload() {
      return payload;
    }

    Topic getTopic() {
      return topic;
    }

    void set(Topic topic, T src) {
      this.topic = topic;
      payloadAllocator.setPayload(src, payload);
    }

    @Override
    public String toString() {
      return "BufferEvent [topic=" + topic + ", payload=" + payload + "]";
    }

  }

  /**
   * Collects attributes to build an EventReactor
   * 
   * If EventReactor is subclassed, then also subclass this Builder to add additional attributes
   *
   * @param <T> type of the object to build
   * @param <B> type of the builder
   */
  @SuppressWarnings("unchecked")
  public static class Builder<T, U extends EventReactor<T>, B extends Builder<T, U, B>> {

    private Dispatcher<T> dispatcher;
    private ExceptionConsumer exceptionHandler = System.err::println;
    private PayloadAllocator<T> payloadAllocator;
    private int ringSize = 128;
    private ThreadFactory threadFactory;


    /**
     * Build a new FixpSession object
     * 
     * @return a new session
     */
    public U build() {
      return (U) new EventReactor<T>(this);
    }

    /**
     * Adds a dispatcher thread
     * 
     * @param dispatcher dispatches an event
     * @return this Builder
     */
    public B withDispatcher(Dispatcher<T> dispatcher) {
      this.dispatcher = dispatcher;
      return (B) this;
    }

    /**
     * Adds an exception handler
     * 
     * @param exceptionHandler a handler for exceptions thrown from an inner context
     * @return this Builder
     */
    public B withExceptionConsumer(ExceptionConsumer exceptionHandler) {
      this.exceptionHandler = exceptionHandler;
      return (B) this;
    }

    /**
     * Adds an allocator for the event payload-
     * 
     * @param payloadAllocator allocates payload for an event
     * @return this Builder
     */
    public B withPayloadAllocator(PayloadAllocator<T> payloadAllocator) {
      this.payloadAllocator = payloadAllocator;
      return (B) this;
    }


    /**
     * Sets the size of a ring buffer of events
     * 
     * @param ringSize size of a ring buffer
     * @return this Builder
     */
    public B withRingSize(int ringSize) {
      this.ringSize = ringSize;
      return (B) this;
    }

    /**
     * Adds a task runner
     * 
     * @param threadFactory task runner
     * @return this Builder
     */
    public B withThreadFactory(ThreadFactory threadFactory) {
      this.threadFactory = threadFactory;
      return (B) this;
    }
  }

  private class TimedPostTask extends TimerTask {

    private final T src;
    private final Topic topic;

    public TimedPostTask(Topic topic, T src) {
      super();
      this.topic = topic;
      this.src = src;
    }

    @Override
    public void run() {
      post(topic, src);
    }

  }

  @SuppressWarnings("rawtypes")
  public static Builder builder() {
    return new Builder();
  }

  private final Dispatcher<T> dispatcher;
  private Disruptor<BufferEvent> disruptor;
  final EventFactory<BufferEvent> EVENT_FACTORY = BufferEvent::new;
  protected final ExceptionConsumer exceptionConsumer;
  private final AtomicBoolean isRunning = new AtomicBoolean();
  private String name = "default";
  private final PayloadAllocator<T> payloadAllocator;
  private final ConcurrentHashMap<Topic, Receiver> registry = new ConcurrentHashMap<>();
  private RingBuffer<BufferEvent> ringBuffer;
  private final int ringSize;
  private final ThreadFactory threadFactory;
  private Timer timer;
  private boolean trace = false;

  protected EventReactor(Builder<T, ?, ?> builder) {
    Objects.requireNonNull(builder.payloadAllocator);
    Objects.requireNonNull(builder.dispatcher);

    this.ringSize = builder.ringSize;
    this.payloadAllocator = builder.payloadAllocator;
    this.dispatcher = builder.dispatcher;
    this.exceptionConsumer = builder.exceptionHandler;
    if (builder.threadFactory != null) {
      this.threadFactory = builder.threadFactory;
    } else {
      this.threadFactory = Executors.defaultThreadFactory();
    }
  }

  /**
   * Stop dispatching events
   */
  public void close() {
    if (isRunning.compareAndSet(true, false)) {
      this.timer.cancel();
      this.disruptor.halt();
      registry.clear();
    }
  }

  private Receiver getSubscriber(Topic topic) {
    return registry.get(topic);
  }

  private void handleEvent(BufferEvent event, long sequence, boolean endOfBatch) {
    Topic topic = event.getTopic();
    Receiver receiver = getSubscriber(topic);
    if (receiver != null) {
      if (trace) {
        System.out.format("Dispatch [%s] %s\n", name, topic);
      }
      try {
        dispatcher.dispatch(event.getTopic(), event.getPayload(), receiver);
      } catch (IOException e) {
        exceptionConsumer.accept(e);
      }
    } else if (trace) {
      System.out.format("Dispatch failed [%s] %s; no subscriber\n", name, topic);
    }
  }

  /**
   * Tells whether a Topic has a subscriber
   * 
   * @param topic key to events
   * @return Returns {@code true} if a subscriber exists for the Topic
   */
  public boolean hasSubscriber(Topic topic) {
    return registry.containsKey(topic);
  }

  /**
   * Start dispatching events
   * 
   * @return a Future that asyncrhonously notifies an observer when this EventReactor is ready
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<? extends EventReactor<T>> open() {
    if (isRunning.compareAndSet(false, true)) {
      CompletableFuture<EventReactor<T>> future = new CompletableFuture<>();
      this.disruptor =
          new Disruptor<>(EVENT_FACTORY, ringSize, threadFactory, ProducerType.MULTI,
              new BusySpinWaitStrategy());
      this.disruptor.handleEventsWith(this::handleEvent);
      this.ringBuffer = disruptor.start();

      // Starts a timer thread
      this.timer = new Timer();
      future.complete(this);
      return future;
    } else {
      return CompletableFuture.completedFuture(this);
    }
  }

  /**
   * Publish an event
   * 
   * @param topic
   *          key to event
   * @param src
   *          message to publish. The expectation for position is similar to
   *          {@code WritableByteChannel.write()} in that the message is found from the start of
   *          buffer to its limit.
   */
  public void post(Topic topic, T src) {
    long sequence = ringBuffer.next();
    final BufferEvent event = ringBuffer.get(sequence);
    event.set(topic, src);
    ringBuffer.publish(sequence);
  }

  /**
   * Publish an event at a specific time
   * 
   * @param topic key to event
   * @param src message to publish
   * @param time time to publish the event
   * @return a TimerSchedule that is used to cancel the timer task
   */
  public TimerSchedule postAt(Topic topic, T src, Date time) {
    final TimedPostTask task = new TimedPostTask(topic, src);
    final TimerSchedule sched = new TimerSchedule(task);
    timer.schedule(task, time);
    return sched;
  }

  /**
   * Publish an event once after a delay
   * 
   * @param topic key to event
   * @param src message to publish
   * @param delayMillis time period in milliseconds
   * @return a TimerSchedule that is used to cancel the timer task
   */
  public TimerSchedule postAt(Topic topic, T src, long delayMillis) {
    final TimedPostTask task = new TimedPostTask(topic, src);
    final TimerSchedule sched = new TimerSchedule(task);
    timer.schedule(task, delayMillis);
    return sched;
  }

  /**
   * Publish an event at regular intervals. The delay to the first event is the same as for
   * subsequent intervals.
   * 
   * @param topic key to event
   * @param src message to publish
   * @param periodMillis time period in milliseconds
   * @return a TimerSchedule that is used to cancel the timer task
   */
  public TimerSchedule postAtInterval(Topic topic, T src, long periodMillis) {
    final TimedPostTask task = new TimedPostTask(topic, src);
    final TimerSchedule sched = new TimerSchedule(task);
    timer.scheduleAtFixedRate(task, periodMillis, periodMillis);
    return sched;
  }

  /**
   * @param trace the trace to set
   */
  public void setTrace(boolean trace) {
    this.trace = trace;
  }

  public void setTrace(boolean trace, String name) {
    this.trace = trace;
    this.name = name;
  }

  /**
   * Subscribe to events published on a Topic.
   * 
   * @param topic key to events
   * @param receiver message handler
   * @return a Subscription object that is used to unsubscribe, or {@code null} if subscription
   *         failed because another Receiver was already registered.
   */
  public Subscription subscribe(Topic topic, Receiver receiver) {
    Objects.requireNonNull(topic);
    Objects.requireNonNull(receiver);
    Receiver theReceiver = registry.putIfAbsent(topic, receiver);
    if (theReceiver == null || receiver == theReceiver) {
      if (trace) {
        System.out.format("Subscribe [%s] %s\n", name, topic);
      }
      return new Subscription(topic, this);
    } else {
      if (trace) {
        System.out.format("Subscribe failed [%s] %s; subscription already exists for topic\n",
            name, topic);
      }
      return null;
    }
  }

  /**
   * Unsubscribe to events published on a Topic
   * 
   * @param topic key to events
   */
  public void unsubscribe(Topic topic) {
    if (trace) {
      System.out.format("Unsubscribe [%s] %s\n", name, topic);
    }
    registry.remove(topic);
  }
}
