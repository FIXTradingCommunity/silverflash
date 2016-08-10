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

import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.PEER_HEARTBEAT;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.PEER_TERMINATED;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.frame.MessageFrameEncoder;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.TimerSchedule;
import org.fixtrading.silverflash.reactor.Topic;

/**
 * A flow that does not accept application messages
 * 
 * @author Don Mendelson
 *
 */
public class NoneFlowReceiver extends AbstractReceiverFlow implements FlowReceiver {

  @SuppressWarnings("rawtypes")
  public static class Builder<T extends NoneFlowReceiver, B extends FlowReceiverBuilder<NoneFlowReceiver, B>>
      extends AbstractReceiverFlow.Builder implements FlowReceiverBuilder {

    public NoneFlowReceiver build() {
      return new NoneFlowReceiver(this);
    }
  }

  public static Builder<NoneFlowReceiver, ? extends FlowReceiverBuilder> builder() {
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

  protected NoneFlowReceiver(Builder builder) {
    super(builder);
    terminatedTopic = SessionEventTopics.getTopic(sessionId, PEER_TERMINATED);

    if (keepaliveInterval != 0) {
      final Topic heartbeatTopic = SessionEventTopics.getTopic(sessionId, PEER_HEARTBEAT);
      heartbeatSubscription = reactor.subscribe(heartbeatTopic, heartbeatEvent);
      heartbeatSchedule = reactor.postAtInterval(heartbeatTopic, null, keepaliveInterval);
    } else {
      // No inbound heartbeats if multicast
      heartbeatSubscription = null;
      heartbeatSchedule = null;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.util.function.Consumer#accept(java.lang.Object)
   */
  public void accept(ByteBuffer buffer) {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.fixtrading.silverflash.fixp.flow.FlowReceiver#isHeartbeatDue()
   */
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
