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
import static org.fixtrading.silverflash.fixp.SessionEventTopics.ToSessionEventType.APPLICATION_MESSAGE_TO_SEND;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.Sequenced;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.messages.ContextDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderEncoder;
import org.fixtrading.silverflash.fixp.messages.NotAppliedDecoder;
import org.fixtrading.silverflash.fixp.messages.NotAppliedEncoder;
import org.fixtrading.silverflash.fixp.messages.SequenceDecoder;
import org.fixtrading.silverflash.fixp.messages.TerminateEncoder;
import org.fixtrading.silverflash.frame.MessageFrameEncoder;
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
  
  private final ContextDecoder contextDecoder = new ContextDecoder();

  private final Receiver heartbeatEvent = buffer -> {
    if (isHeartbeatDue()) {
      // **** terminated(null);
    }
  };
  private final TimerSchedule heartbeatSchedule;
  private final Subscription heartbeatSubscription;
  private final DirectBuffer immutableBuffer = new UnsafeBuffer(new byte[0]);
  private boolean isEndOfStream = false;
  private final AtomicBoolean isHeartbeatDue = new AtomicBoolean(true);
  private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private final AtomicLong nextSeqNoAccepted = new AtomicLong(1);
  private final AtomicLong nextSeqNoReceived = new AtomicLong(1);
  private final NotAppliedEncoder notAppliedEncoder = new NotAppliedEncoder();
  private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(32)
      .order(ByteOrder.nativeOrder());
  private final SequenceDecoder sequenceDecoder = new SequenceDecoder();
  private final Topic terminatedTopic;
  private final Topic toSendTopic;
  private final MutableDirectBuffer mutableBuffer = new UnsafeBuffer(sendBuffer);
  

  @SuppressWarnings("rawtypes")
  protected IdempotentFlowReceiver(Builder builder) {
    super(builder);
    Objects.requireNonNull(messageConsumer);
    int offset = 0;
    frameEncoder.wrap(sendBuffer, offset).encodeFrameHeader();
    offset += frameEncoder.getHeaderLength();
    messageHeaderEncoder.wrap(mutableBuffer, offset);
    messageHeaderEncoder.blockLength(notAppliedEncoder.sbeBlockLength())
        .templateId(notAppliedEncoder.sbeTemplateId()).schemaId(notAppliedEncoder.sbeSchemaId())
        .version(notAppliedEncoder.sbeSchemaVersion());
    offset += messageHeaderEncoder.encodedLength();
    notAppliedEncoder.wrap(mutableBuffer, offset);
    notAppliedEncoder.fromSeqNo(0);
    notAppliedEncoder.count(0);
    frameEncoder.setMessageLength(offset + notAppliedEncoder.encodedLength());
    frameEncoder.encodeFrameTrailer();

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
    immutableBuffer.wrap(buffer);
    int offset = buffer.position();
    messageHeaderDecoder.wrap(immutableBuffer, offset);
    offset += messageHeaderDecoder.encodedLength();
    boolean isApplicationMessage = true;

    int sbeSchemaId = messageHeaderDecoder.schemaId();
    if (sbeSchemaId == sequenceDecoder.sbeSchemaId()) {
 
      switch (messageHeaderDecoder.templateId()) {
      case SequenceDecoder.TEMPLATE_ID:
        sequenceDecoder.wrap(immutableBuffer, offset,
            sequenceDecoder.sbeBlockLength(), sequenceDecoder.sbeSchemaVersion());
        onSequence(sequenceDecoder);
        isApplicationMessage = false;
        break;
      case ContextDecoder.TEMPLATE_ID:
        contextDecoder.wrap(immutableBuffer, offset,
            contextDecoder.sbeBlockLength(), contextDecoder.sbeSchemaVersion());
        onContext(contextDecoder);
        isApplicationMessage = false;
        break;
      case NotAppliedDecoder.TEMPLATE_ID:
        // System.out.println("NotApplied received");
        break;
      case TerminateEncoder.TEMPLATE_ID:
        terminated(buffer);
        isApplicationMessage = false;
        break;
      default:
//        System.out.println("IdempotentFlowReceiver: Protocol violation; unexpected session message "
//            + decoder.getMessageType());
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

  @Override
  public boolean isHeartbeatDue() {
    return isHeartbeatDue.getAndSet(true);
  }

  void notifyGap(long fromSeqNo, int count) {
    // System.out.println("Gap detected");
    notAppliedEncoder.fromSeqNo(fromSeqNo);
    notAppliedEncoder.count(count);
    // Post this to reactor for async sending as an application message
    reactor.post(toSendTopic, sendBuffer.duplicate());
  }

  void onContext(ContextDecoder contextDecoder) {
    final long newNextSeqNo = contextDecoder.nextSeqNo();
    handleSequence(newNextSeqNo);
  }

  void onSequence(SequenceDecoder sequenceDecoder) {
    final long newNextSeqNo = sequenceDecoder.nextSeqNo();
    handleSequence(newNextSeqNo);
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
