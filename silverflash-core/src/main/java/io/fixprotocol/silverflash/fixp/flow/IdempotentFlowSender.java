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

package io.fixprotocol.silverflash.fixp.flow;

import static io.fixprotocol.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_SUSPENDED;
import static io.fixprotocol.silverflash.fixp.SessionEventTopics.SessionEventType.HEARTBEAT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import io.fixprotocol.silverflash.Receiver;
import io.fixprotocol.silverflash.fixp.SessionEventTopics;
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderEncoder;
import io.fixprotocol.silverflash.fixp.messages.TerminateEncoder;
import io.fixprotocol.silverflash.fixp.messages.TerminationCode;
import io.fixprotocol.silverflash.reactor.Subscription;
import io.fixprotocol.silverflash.reactor.TimerSchedule;
import io.fixprotocol.silverflash.reactor.Topic;

/**
 * Sends messages on an idempotent flow on a Transport that guarantees FIFO delivery. The
 * implementation sends a Sequence message only at startup and for heartbeats.
 * 
 * @author Don Mendelson
 *
 */
public class IdempotentFlowSender extends AbstractFlow implements FlowSender, MutableSequence {

  @SuppressWarnings("rawtypes")
  public static class Builder<T extends IdempotentFlowSender, B extends FlowBuilder>
      extends AbstractFlow.Builder {

     public IdempotentFlowSender build() {
      return new IdempotentFlowSender(this);
    }

  }

  private static final ByteBuffer[] EMPTY = new ByteBuffer[0];

  public static  Builder<IdempotentFlowSender, FlowBuilder> builder() {
    return new Builder();
  }

  private final AtomicBoolean criticalSection = new AtomicBoolean();
  private final Receiver heartbeatEvent = t -> {
    try {
      sendHeartbeat();
    } catch (IOException e) {
      try {
        sendEndOfStream();
      } catch (IOException e1) {

      }
      Topic terminatedTopic = SessionEventTopics.getTopic(sessionId, SESSION_SUSPENDED);
      reactor.post(terminatedTopic, t);
    }
  };
  private final TimerSchedule heartbeatSchedule;
  private final Subscription heartbeatSubscription;
  private final AtomicBoolean isHeartbeatDue = new AtomicBoolean(true);
  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private final ByteBuffer[] one = new ByteBuffer[1];
  private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(48)
      .order(ByteOrder.nativeOrder());
  private final TerminateEncoder terminateEncoder = new TerminateEncoder();
  private final MutableDirectBuffer mutableBuffer = new UnsafeBuffer(sendBuffer);

  protected IdempotentFlowSender(@SuppressWarnings("rawtypes") Builder builder) {
    super(builder);

    final Topic heartbeatTopic = SessionEventTopics.getTopic(sessionId, HEARTBEAT);
    heartbeatSubscription = reactor.subscribe(heartbeatTopic, heartbeatEvent);
    heartbeatSchedule = reactor.postAtInterval(heartbeatTopic, ByteBuffer.allocate(0),
        keepaliveInterval);
  }

  /*
   * (non-Javadoc)
   * 
   * @see io.fixprotocol.silverflash.Sequenced#getNextSeqNo()
   */
  public long getNextSeqNo() {
    return sequencer.getNextSeqNo();
  }

  protected boolean isHeartbeatDue() {
    return isHeartbeatDue.getAndSet(true);
  }

  @Override
  public long send(ByteBuffer message) throws IOException {
    Objects.requireNonNull(message);
    one[0] = message;
    return send(one);
  }

  public long send(ByteBuffer[] messages) throws IOException {
    Objects.requireNonNull(messages);
    while (!criticalSection.compareAndSet(false, true)) {
      Thread.yield();
    }
    try {
      transport.write(sequencer.apply(messages));
      isHeartbeatDue.set(false);
      return sequencer.getNextSeqNo();
    } finally {
      criticalSection.compareAndSet(true, false);
    }
  }

  public void sendEndOfStream() throws IOException {
    heartbeatSchedule.cancel();
    heartbeatSubscription.unsubscribe();

    int offset = 0;
    frameEncoder.wrap(sendBuffer, offset).encodeFrameHeader();
    offset += frameEncoder.getHeaderLength();
    messageHeaderEncoder.wrap(mutableBuffer, offset);
    messageHeaderEncoder.blockLength(terminateEncoder.sbeBlockLength())
        .templateId(terminateEncoder.sbeTemplateId()).schemaId(terminateEncoder.sbeSchemaId())
        .version(terminateEncoder.sbeSchemaVersion());
    offset += messageHeaderEncoder.encodedLength();
    terminateEncoder.wrap(mutableBuffer, offset);
    for (int i = 0; i < 16; i++) {
      terminateEncoder.sessionId(i, uuidAsBytes[i]);
    }
    terminateEncoder.code(TerminationCode.Finished);
    frameEncoder.setMessageLength(offset + terminateEncoder.encodedLength());
    frameEncoder.encodeFrameTrailer();

    transport.write(sendBuffer);
  }

  public void sendHeartbeat() throws IOException {
    if (isHeartbeatDue()) {
      send(EMPTY);
    }
  }

  /**
   * Alters sequence for test purposes only!
   */
  public void setNextSeqNo(long nextSeqNo) {
    ((MutableSequence) (this.sequencer)).setNextSeqNo(nextSeqNo);
  }
}
