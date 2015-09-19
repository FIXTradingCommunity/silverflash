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

import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.*;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.fixtrading.silverflash.MessageConsumer;
import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.Session;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.SessionId;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.Decoder;
import org.fixtrading.silverflash.reactor.EventReactor;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.TimerSchedule;
import org.fixtrading.silverflash.reactor.Topic;

/**
 * Receives an unsequenced flow of messages
 * 
 * @author Don Mendelson
 *
 */
public class UnsequencedFlowReceiver implements FlowReceiver {

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
  private final EventReactor<ByteBuffer> reactor;
  private final Session<UUID> session;
  private final UUID sessionId;
  private final MessageConsumer<UUID> streamReceiver;
  private final Topic terminatedTopic;
  private final byte[] uuidAsBytes;

  /**
   * Constructor
   * 
   * @param reactor an EventReactor
   * @param session a session using this flow
   * @param streamReceiver a consumer of application messages
   * @param inboundKeepaliveInterval expected heartbeat interval
   */
  public UnsequencedFlowReceiver(EventReactor<ByteBuffer> reactor, Session<UUID> session,
      MessageConsumer<UUID> streamReceiver, int inboundKeepaliveInterval) {
    Objects.requireNonNull(session);
    Objects.requireNonNull(streamReceiver);
    this.reactor = reactor;
    this.session = session;
    this.sessionId = session.getSessionId();
    uuidAsBytes = SessionId.UUIDAsBytes(sessionId);
    this.streamReceiver = streamReceiver;
    terminatedTopic = SessionEventTopics.getTopic(sessionId, PEER_TERMINATED);

    final Topic heartbeatTopic = SessionEventTopics.getTopic(sessionId, PEER_HEARTBEAT);
    heartbeatSubscription = reactor.subscribe(heartbeatTopic, heartbeatEvent);
    heartbeatSchedule = reactor.postAtInterval(heartbeatTopic, null, inboundKeepaliveInterval);
  }

  @Override
  public void accept(ByteBuffer buffer) {
    Optional<Decoder> optDecoder = messageDecoder.attachForDecode(buffer, buffer.position());

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
          System.err.println("Protocol violation");
      }
    }
    if (isApplicationMessage && !isEndOfStream) {
      isHeartbeatDue.set(false);
      streamReceiver.accept(buffer, session, 0);
    }
  }

  @Override
  public boolean isHeartbeatDue() {
    return isHeartbeatDue.getAndSet(true);
  }

  private void heartbeatReceived() {
    isHeartbeatDue.set(false);
  }

  private void terminated(ByteBuffer buffer) {
    isEndOfStream = true;
    reactor.post(terminatedTopic, buffer);
    heartbeatSchedule.cancel();
    heartbeatSubscription.unsubscribe();
  }
}
