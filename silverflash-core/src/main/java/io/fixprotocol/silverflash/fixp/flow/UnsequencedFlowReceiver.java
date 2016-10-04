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

import static io.fixprotocol.silverflash.fixp.SessionEventTopics.SessionEventType.PEER_HEARTBEAT;
import static io.fixprotocol.silverflash.fixp.SessionEventTopics.SessionEventType.PEER_TERMINATED;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import io.fixprotocol.silverflash.Receiver;
import io.fixprotocol.silverflash.fixp.SessionEventTopics;
import io.fixprotocol.silverflash.fixp.flow.NoneFlowReceiver.Builder;
import io.fixprotocol.silverflash.fixp.messages.MessageHeaderDecoder;
import io.fixprotocol.silverflash.fixp.messages.TerminateDecoder;
import io.fixprotocol.silverflash.fixp.messages.UnsequencedHeartbeatDecoder;
import io.fixprotocol.silverflash.frame.MessageFrameEncoder;
import io.fixprotocol.silverflash.reactor.Subscription;
import io.fixprotocol.silverflash.reactor.TimerSchedule;
import io.fixprotocol.silverflash.reactor.Topic;

/**
 * Receives an unsequenced flow of messages
 * 
 * @author Don Mendelson
 *
 */
public class UnsequencedFlowReceiver extends AbstractReceiverFlow implements FlowReceiver {

  @SuppressWarnings("rawtypes")
  public static class Builder<T extends UnsequencedFlowReceiver, B extends FlowReceiverBuilder<UnsequencedFlowReceiver, B>>
      extends AbstractReceiverFlow.Builder implements FlowReceiverBuilder {

    public UnsequencedFlowReceiver build() {
      return new UnsequencedFlowReceiver(this);
    }

  }

  public static Builder<UnsequencedFlowReceiver, ? extends FlowReceiverBuilder> builder() {
    return new Builder();
  }

  private final Receiver heartbeatEvent = t -> {
    if (isHeartbeatDue()) {
      terminated(null);
    }
  };

  private final TimerSchedule heartbeatSchedule;
  private final Subscription heartbeatSubscription;
  private boolean isEndOfStream = false;
  private final AtomicBoolean isHeartbeatDue = new AtomicBoolean(true);
  private final Topic terminatedTopic;
  private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
  private final DirectBuffer immutableBuffer = new UnsafeBuffer(new byte[0]);


  protected UnsequencedFlowReceiver(Builder builder) {
    super(builder);
    Objects.requireNonNull(messageConsumer);

    terminatedTopic = SessionEventTopics.getTopic(sessionId, PEER_TERMINATED);

    final Topic heartbeatTopic = SessionEventTopics.getTopic(sessionId, PEER_HEARTBEAT);
    heartbeatSubscription = reactor.subscribe(heartbeatTopic, heartbeatEvent);
    heartbeatSchedule = reactor.postAtInterval(heartbeatTopic, null, keepaliveInterval);
  }

  @Override
  public void accept(ByteBuffer buffer) {
    boolean isApplicationMessage = true;
    immutableBuffer.wrap(buffer);
    int offset = buffer.position();
    messageHeaderDecoder.wrap(immutableBuffer, offset);
    if (messageHeaderDecoder.schemaId() == TerminateDecoder.SCHEMA_ID) {

      switch (messageHeaderDecoder.templateId()) {
      case UnsequencedHeartbeatDecoder.TEMPLATE_ID:
        heartbeatReceived();
        isApplicationMessage = false;
        break;
      case TerminateDecoder.TEMPLATE_ID:
        terminated(buffer);
        isApplicationMessage = false;
        break;
      default:
//        System.out.println("UnsequencedFlowReceiver: Protocol violation; unexpected session message "
//            + decoder.getMessageType());
        reactor.post(terminatedTopic, buffer);
      }
    }
    if (isApplicationMessage && !isEndOfStream) {
      isHeartbeatDue.set(false);
      messageConsumer.accept(buffer, session, 0);
    }
  }

  private void heartbeatReceived() {
    isHeartbeatDue.set(false);
  }

  @Override
  public boolean isHeartbeatDue() {
    return isHeartbeatDue.getAndSet(true);
  }

  private void terminated(ByteBuffer buffer) {
    isEndOfStream = true;
    reactor.post(terminatedTopic, buffer);
    heartbeatSchedule.cancel();
    heartbeatSubscription.unsubscribe();
  }
}
