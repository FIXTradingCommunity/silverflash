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

package org.fixtrading.silverflash.fixp.flow;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_SUSPENDED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.HEARTBEAT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.flow.NoneFlowReceiver.Builder;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderEncoder;
import org.fixtrading.silverflash.fixp.messages.TerminateEncoder;
import org.fixtrading.silverflash.fixp.messages.TerminationCode;
import org.fixtrading.silverflash.fixp.messages.UnsequencedHeartbeatEncoder;
import org.fixtrading.silverflash.frame.MessageFrameEncoder;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.TimerSchedule;
import org.fixtrading.silverflash.reactor.Topic;

/**
 * Sends messages on an unsequenced flow.
 * 
 * @author Don Mendelson
 *
 */
public class UnsequencedFlowSender extends AbstractFlow implements FlowSender {

  @SuppressWarnings("rawtypes")
  public static class Builder<T extends UnsequencedFlowSender, B extends FlowBuilder>
      extends AbstractFlow.Builder {

    @SuppressWarnings("unchecked")
    public T build() {
      return (T) new UnsequencedFlowSender(this);
    }
  }

  @SuppressWarnings("rawtypes")
  public static Builder builder() {
    return new Builder();
  }

  private final Receiver heartbeatEvent = t -> {
    try {
      sendHeartbeat();
    } catch (IOException e) {
      Topic terminatedTopic = SessionEventTopics.getTopic(sessionId, SESSION_SUSPENDED);
      reactor.post(terminatedTopic, t);
    }
  };
  private final TimerSchedule heartbeatSchedule;

  private final Subscription heartbeatSubscription;
  private final AtomicBoolean isHeartbeatDue = new AtomicBoolean(true);
  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(48)
      .order(ByteOrder.nativeOrder());
  private final TerminateEncoder terminateEncoder = new TerminateEncoder();
  private final UnsequencedHeartbeatEncoder unsequencedHeartbeatEncoder = new UnsequencedHeartbeatEncoder();
  private final MutableDirectBuffer mutableBuffer = new UnsafeBuffer(sendBuffer);

  protected UnsequencedFlowSender(Builder builder) {
    super(builder);
    messageHeaderEncoder.wrap(mutableBuffer, 0);
    messageHeaderEncoder.blockLength(unsequencedHeartbeatEncoder.sbeBlockLength())
        .templateId(unsequencedHeartbeatEncoder.sbeTemplateId()).schemaId(unsequencedHeartbeatEncoder.sbeSchemaId())
        .version(unsequencedHeartbeatEncoder.sbeSchemaVersion());
    unsequencedHeartbeatEncoder.wrap(mutableBuffer, 0);
    final Topic heartbeatTopic = SessionEventTopics.getTopic(sessionId, HEARTBEAT);
    heartbeatSubscription = reactor.subscribe(heartbeatTopic, heartbeatEvent);
    heartbeatSchedule = reactor.postAtInterval(heartbeatTopic, sendBuffer,
        keepaliveInterval);
  }

  @Override
  public long send(ByteBuffer message) throws IOException {
    Objects.requireNonNull(message);
    transport.write(message);
    isHeartbeatDue.set(false);
    return 0;
  }

  @Override
  public long send(ByteBuffer[] messages) throws IOException {
    Objects.requireNonNull(messages);
    transport.write(messages);
    isHeartbeatDue.set(false);
    return 0;
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
    if (isHeartbeatDue.getAndSet(true)) {
      transport.write(sendBuffer);
    }
  }
}
