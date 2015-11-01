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

import static org.fixtrading.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_SUSPENDED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.HEARTBEAT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.TerminateEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageType;
import org.fixtrading.silverflash.fixp.messages.TerminationCode;
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

  private final ByteBuffer heartbeatBuffer = ByteBuffer.allocateDirect(10)
      .order(ByteOrder.nativeOrder());
  private final Receiver heartbeatEvent = new Receiver() {

    public void accept(ByteBuffer t) {
      try {
        sendHeartbeat();
      } catch (IOException e) {
        Topic terminatedTopic = SessionEventTopics.getTopic(sessionId, SESSION_SUSPENDED);
        reactor.post(terminatedTopic, t);
      }
    }

  };

  private final TimerSchedule heartbeatSchedule;
  private final Subscription heartbeatSubscription;
  private final AtomicBoolean isHeartbeatDue = new AtomicBoolean(true);

  protected UnsequencedFlowSender(Builder builder) {
    super(builder);
    messageEncoder.wrap(heartbeatBuffer, 0, MessageType.UNSEQUENCED_HEARTBEAT);

    final Topic heartbeatTopic = SessionEventTopics.getTopic(sessionId, HEARTBEAT);
    heartbeatSubscription = reactor.subscribe(heartbeatTopic, heartbeatEvent);
    heartbeatSchedule = reactor.postAtInterval(heartbeatTopic, heartbeatBuffer,
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

    final ByteBuffer terminateBuffer = ByteBuffer.allocateDirect(29).order(ByteOrder.nativeOrder());
    TerminateEncoder terminateEncoder = (TerminateEncoder) messageEncoder.wrap(terminateBuffer, 0,
        MessageType.TERMINATE);
    terminateEncoder.setSessionId(uuidAsBytes);
    terminateEncoder.setCode(TerminationCode.FINISHED);
    terminateEncoder.setReasonNull();
    transport.write(terminateBuffer);
  }

  public void sendHeartbeat() throws IOException {
    if (isHeartbeatDue.getAndSet(true)) {
      transport.write(heartbeatBuffer);
    }
  }
}
