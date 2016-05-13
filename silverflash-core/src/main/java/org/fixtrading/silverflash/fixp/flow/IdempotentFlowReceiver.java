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
import static org.fixtrading.silverflash.fixp.SessionEventTopics.ToSessionEventType.APPLICATION_MESSAGE_TO_SEND;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.Sequenced;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.ContextDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.Decoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.SequenceDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.NotAppliedEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageType;
import org.fixtrading.silverflash.fixp.messages.SbeMessageHeaderDecoder;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.TimerSchedule;
import org.fixtrading.silverflash.reactor.Topic;

/**
 * Receives an idempotent message flow
 * 
 * @author Don Mendelson
 *
 */
public class IdempotentFlowReceiver extends AbstractReceiverFlow implements FlowReceiver, Sequenced {

  @SuppressWarnings("rawtypes")
  public static class Builder<T extends IdempotentFlowReceiver, B extends FlowReceiverBuilder<IdempotentFlowReceiver, B>>
      extends AbstractReceiverFlow.Builder implements FlowReceiverBuilder  {

    public IdempotentFlowReceiver build() {
      return new IdempotentFlowReceiver(this);
    }
   }

  public static  Builder<IdempotentFlowReceiver, ? extends FlowReceiverBuilder> builder() {
    return new Builder();
  }
  
  private final Receiver heartbeatEvent = buffer -> {
    if (isHeartbeatDue()) {
      // **** terminated(null);
    }
  };

  private final TimerSchedule heartbeatSchedule;
  private final Subscription heartbeatSubscription;
  private boolean isEndOfStream = false;
  private final AtomicBoolean isHeartbeatDue = new AtomicBoolean(true);
  private final MessageDecoder messageDecoder = new MessageDecoder();
  private final AtomicLong nextSeqNoAccepted = new AtomicLong(1);
  private final AtomicLong nextSeqNoReceived = new AtomicLong(1);
  private final ByteBuffer notAppliedBuffer = ByteBuffer.allocateDirect(32)
      .order(ByteOrder.nativeOrder());
  private final NotAppliedEncoder notAppliedEncoder;
  private final Topic terminatedTopic;
  private final Topic toSendTopic;

  @SuppressWarnings("rawtypes")
  protected IdempotentFlowReceiver(Builder builder) {
    super(builder);
    Objects.requireNonNull(messageConsumer);
    notAppliedEncoder = (NotAppliedEncoder) messageEncoder.wrap(notAppliedBuffer, 0,
        MessageType.NOT_APPLIED);
    notAppliedEncoder.setFromSeqNo(0);
    notAppliedEncoder.setCount(0);
    toSendTopic = SessionEventTopics.getTopic(sessionId, APPLICATION_MESSAGE_TO_SEND);
    terminatedTopic = SessionEventTopics.getTopic(sessionId, PEER_TERMINATED);

    if (this.keepaliveInterval != 0) {
      final Topic heartbeatTopic = SessionEventTopics.getTopic(sessionId, PEER_HEARTBEAT);
      heartbeatSubscription = reactor.subscribe(heartbeatTopic, heartbeatEvent);
      heartbeatSchedule = reactor.postAtInterval(heartbeatTopic, null, this.keepaliveInterval);
    } else {
      heartbeatSubscription = null;
      heartbeatSchedule = null;
    }
  }

  public void accept(ByteBuffer buffer) {
    Optional<Decoder> optDecoder = messageDecoder.wrap(buffer, buffer.position());

    boolean isApplicationMessage = true;
    if (optDecoder.isPresent()) {
      final Decoder decoder = optDecoder.get();
      switch (decoder.getMessageType()) {
      case SEQUENCE:
        accept((SequenceDecoder) decoder);
        isApplicationMessage = false;
        break;
      case CONTEXT:
        accept((ContextDecoder) decoder);
        isApplicationMessage = false;
        break;
      case NOT_APPLIED:
        // System.out.println("NotApplied received");
        break;
      case TERMINATE:
        terminated(buffer);
        isApplicationMessage = false;
        break;
      default:
//        System.out.println("IdempotentFlowReceiver: Protocol violation; unexpected session message "
//            + decoder.getMessageType());
        buffer.reset();
        reactor.post(terminatedTopic, buffer);
      }
    }
    if (isApplicationMessage && !isEndOfStream) {
      // if not a known message type, assume it's an application message
      final long seqNo = nextSeqNoReceived.getAndIncrement();
      if (nextSeqNoAccepted.compareAndSet(seqNo, seqNo)) {
        nextSeqNoAccepted.incrementAndGet();
        messageConsumer.accept(buffer, session, seqNo);
      }
    }
  }

  public long getNextSeqNo() {
    return nextSeqNoReceived.get();
  }

  @Override
  public boolean isHeartbeatDue() {
    return isHeartbeatDue.getAndSet(true);
  }

  void accept(ContextDecoder contextDecoder) {
    final long newNextSeqNo = contextDecoder.getNextSeqNo();
    handleSequence(newNextSeqNo);
  }

  void accept(SequenceDecoder sequenceDecoder) {
    final long newNextSeqNo = sequenceDecoder.getNextSeqNo();
    handleSequence(newNextSeqNo);
  }

  void notifyGap(long fromSeqNo, int count) {
    // System.out.println("Gap detected");
    notAppliedEncoder.setFromSeqNo(fromSeqNo);
    notAppliedEncoder.setCount(count);
    // Post this to reactor for async sending as an application message
    reactor.post(toSendTopic, notAppliedBuffer.duplicate());
  }

  private void handleSequence(final long newNextSeqNo) {
    isHeartbeatDue.set(false);
    final long prevNextSeqNo = nextSeqNoReceived.getAndSet(newNextSeqNo);
    // todo: protocol violation if less than previous seq?
    final long accepted = nextSeqNoAccepted.get();
    // System.out.format("Seq no: %d; prev nextSeqNo %d; accepted %d\n",
    // newNextSeqNo, prevNextSeqNo, accepted);
    if (newNextSeqNo > accepted) {
      if (newNextSeqNo > accepted) {
        notifyGap(prevNextSeqNo, (int) (newNextSeqNo - prevNextSeqNo));
      }
      // Continue to accept messages after the gap as if the missing
      // messages were received
      nextSeqNoAccepted.set(newNextSeqNo);
    }
  }

  private void terminated(ByteBuffer buffer) {
    isEndOfStream = true;
    reactor.post(terminatedTopic, buffer);
    if (heartbeatSchedule != null) {
      heartbeatSchedule.cancel();
      heartbeatSubscription.unsubscribe();
    }
  }
}
