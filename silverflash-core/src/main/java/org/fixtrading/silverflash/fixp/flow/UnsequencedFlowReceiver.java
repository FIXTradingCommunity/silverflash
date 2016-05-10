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

package org.fixtrading.silverflash.fixp.flow;

import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.PEER_HEARTBEAT;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.PEER_TERMINATED;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.Decoder;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.TimerSchedule;
import org.fixtrading.silverflash.reactor.Topic;

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
  private final MessageDecoder messageDecoder = new MessageDecoder();
  private final Topic terminatedTopic;

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
    Optional<Decoder> optDecoder = messageDecoder.wrap(buffer, buffer.position());

    boolean isApplicationMessage = true;
    if (optDecoder.isPresent()) {
      final Decoder decoder = optDecoder.get();
      switch (decoder.getMessageType()) {
      case UNSEQUENCED_HEARTBEAT:
        heartbeatReceived();
        isApplicationMessage = false;
        break;
      case TERMINATE:
        terminated(buffer);
        isApplicationMessage = false;
        break;
      default:
        // Todo: post to an async handler
        System.out.println("UnsequencedFlowReceiver: Protocol violation; unexpected session message "
            + decoder.getMessageType());
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
