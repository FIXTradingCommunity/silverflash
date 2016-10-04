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
package io.fixprotocol.silverflash.fixp.frame;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import io.fixprotocol.silverflash.fixp.messages.MessageHeaderDecoder;
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderEncoder;
import io.fixprotocol.silverflash.frame.MessageFrameEncoder;
import io.fixprotocol.silverflash.frame.MessageLengthFrameEncoder;
import io.fixprotocol.silverflash.frame.MessageLengthFrameSpliterator;

@State(Scope.Benchmark)
public class FrameSpliteratorBenchmark {

  @Param({"1", "2", "4", "8"})
  public int numberOfMessages;


  @Param({"128", "256", "1024"})
  public int messageLength;

  private final int schemaVersion = 1;
  private final int schemaId = 66;
  private final int templateId = 77;

  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
  private MutableDirectBuffer mutableBuffer = new UnsafeBuffer(new byte[0]);
  private final DirectBuffer immutableBuffer = new UnsafeBuffer(new byte[0]);

  private MessageLengthFrameSpliterator spliterator;
  private MessageFrameEncoder frameEncoder;
  private ByteBuffer buffer;

  @AuxCounters
  @State(Scope.Thread)
  public static class Counters {
    public int failed;
    public int succeeded;

    @Setup(Level.Iteration)
    public void clean() {
      failed = 0;
      succeeded = 0;
    }
  }

  @Setup
  public void initTestEnvironment() throws Exception {
    buffer = ByteBuffer.allocate(16 * 1024).order(ByteOrder.nativeOrder());
    ByteBuffer message =
        ByteBuffer.allocate(messageLength - MessageHeaderDecoder.ENCODED_LENGTH).order(
            ByteOrder.nativeOrder());
    
    spliterator = new MessageLengthFrameSpliterator();
    frameEncoder = new MessageLengthFrameEncoder();

    for (int i = 0; i < message.limit(); i++) {
      message.put((byte) i);
    }

    for (int i = 0; i < numberOfMessages; i++) {
      encodeApplicationMessageWithFrame(buffer, message);
    }
  }

  @Benchmark
  public void parse(Counters counters) throws IOException {
    buffer.rewind();
    spliterator.wrap(buffer);

    while (spliterator.tryAdvance((Consumer<ByteBuffer>) message -> {
      immutableBuffer.wrap(buffer);
      int offset = buffer.position();
      messageHeaderDecoder.wrap(immutableBuffer, offset);

      if (templateId == messageHeaderDecoder.templateId()) {
        counters.succeeded++;
      } else {
        counters.failed++;
      }

    }));
  }

 
  private long encodeApplicationMessageWithFrame(ByteBuffer buffer, ByteBuffer message) {
    int length = message.remaining();
    int offset = 0;
    mutableBuffer.wrap(buffer);
    frameEncoder.wrap(buffer, offset).encodeFrameHeader();
    offset += frameEncoder.getHeaderLength();
    messageHeaderEncoder.wrap(mutableBuffer, offset);
    messageHeaderEncoder.blockLength(length)
        .templateId(templateId).schemaId(schemaId)
        .version(schemaVersion);
    offset += MessageHeaderEncoder.ENCODED_LENGTH; 
    buffer.position(offset);
    buffer.put(message);
    frameEncoder.setMessageLength(offset + length);
    frameEncoder.encodeFrameTrailer();
    return frameEncoder.getEncodedLength();
  }
}
