/**
 *    Copyright 2015-2016 FIX Protocol Ltd
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

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.reactor.ByteBufferDispatcher;
import org.fixtrading.silverflash.reactor.ByteBufferPayload;
import org.fixtrading.silverflash.reactor.EventFuture;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.Topic;
import org.fixtrading.silverflash.reactor.Topics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EventReactorTest {


  class TestReceiver implements Receiver {

    private ByteBuffer lastBuffer;
    private AtomicInteger count = new AtomicInteger();

    @Override
    public void accept(ByteBuffer buffer) {
      this.lastBuffer = buffer;
      this.count.incrementAndGet();
    }

    public int getCount() {
      return count.get();
    }

    public void setCount(int count) {
      this.count.set(count);
    }

    public ByteBuffer getLastBuffer() {
      return lastBuffer;
    }

  }

  EventReactor<ByteBuffer> reactor;
  TestReceiver receiver;

  @Before
  public void setUp() throws Exception {
    receiver = new TestReceiver();
    reactor =
        EventReactor.builder().withDispatcher(new ByteBufferDispatcher())
            .withPayloadAllocator(new ByteBufferPayload(2048)).build();
    reactor.open().get();
  }

  @After
  public void tearDown() throws Exception {
    reactor.close();
  }

  @Test
  public void testSubscribe() {
    Topic topic = Topics.getTopic("TestTopic1");
    Subscription subscription1 = reactor.subscribe(topic, receiver);

    ByteBuffer src = ByteBuffer.allocate(1024);
    final byte[] bytes = "Hello world!".getBytes();
    src.put(bytes);
    reactor.post(topic, src);
    reactor.post(topic, src);

    try {
      Thread.sleep(1000L);
    } catch (InterruptedException e) {

    }

    assertEquals(2, receiver.getCount());
    ByteBuffer received = receiver.getLastBuffer();

    byte[] dst = new byte[bytes.length];
    received.get(dst);
    assertArrayEquals(bytes, dst);


    Topic topic2 = Topics.getTopic("TestTopic2");
    assertTrue(topic.hashCode() != topic2.hashCode());
    reactor.post(topic2, src);
    try {
      Thread.sleep(100L);
    } catch (InterruptedException e) {

    }
    assertEquals(2, receiver.getCount());

    Subscription subscription2 = reactor.subscribe(topic2, receiver);
    reactor.post(topic2, src);
    try {
      Thread.sleep(100L);
    } catch (InterruptedException e) {

    }
    assertEquals(3, receiver.getCount());

    reactor.unsubscribe(topic);
    reactor.post(topic, src);
    try {
      Thread.sleep(100L);
    } catch (InterruptedException e) {

    }
    assertEquals(3, receiver.getCount());
  }

  @Test
  public void future() throws InterruptedException, ExecutionException, TimeoutException {
    Topic topic = Topics.getTopic("TestTopic2");
    EventFuture future = new EventFuture(topic, reactor);
    Thread thread = new Thread(new Runnable() {

      public void run() {
        ByteBuffer src = ByteBuffer.allocate(1024);
        src.put("Hello, world".getBytes());
        reactor.post(topic, src);
      }
    });
    thread.start();
    ByteBuffer result = future.get(1000L, TimeUnit.MILLISECONDS);
    assertTrue(future.isDone() && !future.isCancelled());
  }

  @Test(expected = CancellationException.class)
  public void canceled() throws InterruptedException, ExecutionException, TimeoutException {
    Topic topic = Topics.getTopic("TestTopic2");
    EventFuture future = new EventFuture(topic, reactor);
    Thread thread = new Thread(new Runnable() {

      public void run() {
        future.cancel(false);
      }
    });
    thread.start();
    ByteBuffer result = future.get(1000L, TimeUnit.MILLISECONDS);
  }

  @Test(expected = TimeoutException.class)
  public void timedOut() throws InterruptedException, ExecutionException, TimeoutException {
    Topic topic = Topics.getTopic("TestTopic2");
    EventFuture future = new EventFuture(topic, reactor);
    ByteBuffer result = future.get(10L, TimeUnit.MILLISECONDS);
  }
}
