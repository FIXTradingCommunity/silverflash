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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.TimerSchedule;
import org.fixtrading.silverflash.reactor.Topic;

/**
 * A flow that does not accept application messages
 * 
 * @author Don Mendelson
 *
 */
public class NoneReceiver implements FlowReceiver {

  private final Receiver heartbeatEvent = t -> {
    if (isHeartbeatDue()) {
      terminated(null);
    }
  };

  private final TimerSchedule heartbeatSchedule;
  private final Subscription heartbeatSubscription;
  private boolean isEndOfStream = false;
  private final AtomicBoolean isHeartbeatDue = new AtomicBoolean(true);
  private final EventReactor<ByteBuffer> reactor;
  private final Topic terminatedTopic;
  private final byte[] uuidAsBytes;

  /**
   * Constructor
   * 
   * @param reactor an EventReactor
   * @param sessionId unique session ID
   * @param inboundKeepaliveInterval expected heartbeat interval
   */
  public NoneReceiver(EventReactor<ByteBuffer> reactor, UUID sessionId, int inboundKeepaliveInterval) {
    Objects.requireNonNull(sessionId);

    this.reactor = reactor;
    uuidAsBytes = SessionId.UUIDAsBytes(sessionId);
    terminatedTopic = SessionEventTopics.getTopic(sessionId, PEER_TERMINATED);

    if (inboundKeepaliveInterval != 0) {
      final Topic heartbeatTopic = SessionEventTopics.getTopic(sessionId, PEER_HEARTBEAT);
      heartbeatSubscription = reactor.subscribe(heartbeatTopic, heartbeatEvent);
      heartbeatSchedule = reactor.postAtInterval(heartbeatTopic, null, inboundKeepaliveInterval);
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
