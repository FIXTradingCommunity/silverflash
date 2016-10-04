/**
 * Copyright 2015 FIX Protocol Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package io.fixprotocol.silverflash.reactor;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import io.fixprotocol.silverflash.Receiver;
import io.fixprotocol.silverflash.reactor.ByteBufferDispatcher;
import io.fixprotocol.silverflash.reactor.ByteBufferPayload;
import io.fixprotocol.silverflash.reactor.EventReactor;
import io.fixprotocol.silverflash.reactor.Topic;
import io.fixprotocol.silverflash.reactor.Topics;
import io.fixprotocol.silverflash.util.platform.AffinityThreadFactory;


@State(Scope.Benchmark)
public class EventReactorBenchmark {

  @State(Scope.Thread)
  public static class Publisher {

    private int instance = 0;
    private Integer myInstance;

    @Setup
    public void create() {
      myInstance = sessions.poll();
    }

    public int next() {
      return instance++;
    }
  }

  class TestReceiver implements Receiver {

    private AtomicInteger count = new AtomicInteger();
    private ByteBuffer lastBuffer;

    @Override
    public void accept(ByteBuffer buffer) {
      this.lastBuffer = buffer;
      this.count.incrementAndGet();
    }

    public int getCount() {
      return count.get();
    }

    public ByteBuffer getLastBuffer() {
      return lastBuffer;
    }

    public void setCount(int count) {
      this.count.set(count);
    }

  }

  private static Queue<Integer> sessions = new ArrayDeque<>();

  private int messageLength = 1024;
  private ByteBuffer[] messages;

  @Param({"1", "2"})
  public int numberOfDispatchers;

  @Param({"1", "2"})
  public int numberOfPublishers;

  @Param({"16", "256"})
  public int numberOfTopics;

  private EventReactor<ByteBuffer> reactor;

  private TestReceiver[] receivers;
  @Param({"128", "256", "1024"})
  public int ringSize;
  private final ThreadFactory threadFactory = new AffinityThreadFactory(true, true, "benchmark");
  private Topic[] topics;


  @TearDown
  public void detroyTestEnvironment() {
    reactor.close();
  }

  @SuppressWarnings("unchecked")
  @Setup
  public void initTestEnvironment() throws Exception {
    reactor = EventReactor.builder().withRingSize(ringSize).withThreadFactory(threadFactory)
        .withDispatcher(new ByteBufferDispatcher())
        .withPayloadAllocator(new ByteBufferPayload(2048)).build();
    reactor.open().get();

    topics = new Topic[numberOfTopics];
    receivers = new TestReceiver[numberOfTopics];
    messages = new ByteBuffer[numberOfTopics];

    for (int i = 0; i < numberOfTopics; ++i) {
      topics[i] = Topics.getTopic("Topic" + i);
      receivers[i] = new TestReceiver();
      reactor.subscribe(topics[i], receivers[i]);
      messages[i] = ByteBuffer.allocate(messageLength);
      messages[i].put("Hello World!".getBytes());
    }

    for (int i = 0; i < numberOfDispatchers; ++i) {
      sessions.offer(i);
    }
  }

  @Benchmark
  public void publish(Publisher local) {
    int instance = local.next();
    Topic topic = topics[instance % numberOfTopics];
    reactor.post(topic, messages[instance % numberOfTopics]);
  }
}
