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

package org.fixtrading.silverflash.fixp.frame;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Consumer;

import org.fixtrading.silverflash.fixp.frame.FixpWithMessageLengthFrameSpliterator;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderWithFrame;
import org.junit.Before;
import org.junit.Test;

public class FrameSpliteratorTest {

  FixpWithMessageLengthFrameSpliterator spliterator;
  ByteBuffer buffer;
  private ByteBuffer[] messages;
  final int messageCount = Byte.MAX_VALUE;
  private int count;
  private final int schemaVersion = 1;
  private final int schemaId = 66;
  private final int templateId = 77;
  private MessageHeaderWithFrame messageHeader = new MessageHeaderWithFrame();

  @Before
  public void setUp() throws Exception {
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
    spliterator = new FixpWithMessageLengthFrameSpliterator(buffer);

    count = 0;
    buffer.rewind();
    byte[] dst = new byte[16 * 1024];

    while (spliterator.tryAdvance(new Consumer<ByteBuffer>() {

      public void accept(ByteBuffer message) {
        messageHeader.attachForDecode(buffer, message.position());
        assertEquals(templateId, messageHeader.getTemplateId());
        int messageLength = messageHeader.getMessageLength();
        assertEquals(MessageHeaderWithFrame.getLength() + count + 1, messageLength);
        message.get(dst, 0, messageLength);
        assertEquals(messages[count].get(0), dst[MessageHeaderWithFrame.getLength()]);
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
    spliterator = new FixpWithMessageLengthFrameSpliterator(buffer);
    spliterator.tryAdvance(new Consumer<ByteBuffer>() {

      public void accept(ByteBuffer message) {
        messageHeader.attachForDecode(buffer, message.position());
        assertEquals(schemaId, messageHeader.getSchemaId());
      }
    });
  }

  public void encodeApplicationMessage(ByteBuffer buf, ByteBuffer message) {
    message.rewind();
    int messageLength = message.remaining();
    MessageHeaderWithFrame.encode(buf, buf.position(), messageLength, templateId, schemaId,
        schemaVersion, messageLength + MessageHeaderWithFrame.getLength());

    buf.put(message);
  }
}
