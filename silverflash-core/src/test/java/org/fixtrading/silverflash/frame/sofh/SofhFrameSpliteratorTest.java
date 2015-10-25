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

package org.fixtrading.silverflash.frame.sofh;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Consumer;

import org.fixtrading.silverflash.frame.sofh.SofhFrameEncoder;
import org.fixtrading.silverflash.frame.sofh.SofhFrameSpliterator;
import org.junit.Before;
import org.junit.Test;

public class SofhFrameSpliteratorTest {

  SofhFrameSpliterator spliterator;
  ByteBuffer buffer;
  private ByteBuffer[] messages;
  final int messageCount = Byte.MAX_VALUE;
  private int count;
  private SofhFrameEncoder encoder;

  @Before
  public void setUp() throws Exception {
    encoder = new SofhFrameEncoder();

    messages = new ByteBuffer[messageCount];
    for (int i = 0; i < messageCount; ++i) {
      // at least 1 byte per message
      messages[i] = ByteBuffer.allocate(i + 1).order(ByteOrder.nativeOrder());
      for (int j = 0; j < messages[i].limit(); ++j) {
        messages[i].put((byte) i);
      }
    }

    buffer = ByteBuffer.allocate(16 * 1024).order(ByteOrder.nativeOrder());
  }

  @Test
  public void testTryAdvance() {

    for (int i = 0; i < messageCount; ++i) {
      encodeApplicationMessage(buffer, messages[i]);
    }

    buffer.flip();
    spliterator = new SofhFrameSpliterator(buffer);

    count = 0;
    buffer.rewind();
 
    while (spliterator.tryAdvance(new Consumer<ByteBuffer>() {

      public void accept(ByteBuffer message) {
        int messageLength = message.remaining();
        assertEquals(count + 1, messageLength);
        count++;

      }
    }));

    assertEquals(messageCount, count);
  }

  @Test
  public void partialMessage() {
    encodeApplicationMessage(buffer, messages[messageCount - 1]);
    buffer.limit(buffer.position() - 1);
    buffer.flip();
    spliterator = new SofhFrameSpliterator(buffer);
    assertFalse(spliterator.tryAdvance(new Consumer<ByteBuffer>() {

      public void accept(ByteBuffer message) {
      }
    }));
  }

  public void encodeApplicationMessage(ByteBuffer buf, ByteBuffer message) {
    message.rewind();
    int messageLength = message.remaining();
    encoder.wrap(buf).setMessageLength(messageLength).encodeFrameHeader();
    buf.put(message);
  }
}
