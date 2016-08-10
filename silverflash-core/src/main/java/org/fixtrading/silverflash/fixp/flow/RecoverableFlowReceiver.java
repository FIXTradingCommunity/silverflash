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

import static org.fixtrading.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_FINISHED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.SERVICE_STORE_RETREIVE;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.PEER_HEARTBEAT;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.PEER_TERMINATED;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.Sequenced;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.flow.NoneFlowReceiver.Builder;
import org.fixtrading.silverflash.fixp.messages.FinishedSendingDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageHeaderEncoder;
import org.fixtrading.silverflash.fixp.messages.RetransmissionDecoder;
import org.fixtrading.silverflash.fixp.messages.RetransmitRequestEncoder;
import org.fixtrading.silverflash.fixp.messages.SequenceDecoder;
import org.fixtrading.silverflash.fixp.messages.TerminateDecoder;
import org.fixtrading.silverflash.frame.MessageFrameEncoder;
import org.fixtrading.silverflash.reactor.Subscription;
import org.fixtrading.silverflash.reactor.TimerSchedule;
import org.fixtrading.silverflash.reactor.Topic;

/**
 * Receives a recoverable message flow
 * 
 * @author Don Mendelson
 *
 */
public class RecoverableFlowReceiver extends AbstractReceiverFlow
    implements Sequenced, FlowReceiver {

  @SuppressWarnings("rawtypes")
  public static class Builder<T extends RecoverableFlowReceiver, B extends FlowReceiverBuilder<RecoverableFlowReceiver, B>>
      extends AbstractReceiverFlow.Builder implements FlowReceiverBuilder {

    public RecoverableFlowReceiver build() {
      return new RecoverableFlowReceiver(this);
    }

  }

  public static Builder<RecoverableFlowReceiver, ? extends FlowReceiverBuilder> builder() {
    return new Builder();
  }

  private final Topic finishedTopic;

  private final Receiver heartbeatEvent = t -> {
    if (isHeartbeatDue()) {
      terminated(null);
    }
  };
  private final TimerSchedule heartbeatSchedule;
  private final Subscription heartbeatSubscription;
  private boolean isEndOfStream = false;
  private final AtomicBoolean isHeartbeatDue = new AtomicBoolean(true);
  private final AtomicBoolean isRetransmission = new AtomicBoolean();
  private long lastRequestTimestamp = 0L;
  private long lastRetransSeqNoToAccept = 0;
  private final AtomicLong nextRetransSeqNoReceived = new AtomicLong(1);
  private final AtomicLong nextSeqNoAccepted = new AtomicLong(1);
  private final AtomicLong nextSeqNoReceived = new AtomicLong(1);
  private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(64)
      .order(ByteOrder.nativeOrder());
  private final RetransmitRequestEncoder retransmitRequestEncoder = new RetransmitRequestEncoder();
  private final byte[] retransSessionId = new byte[16];
  private final Topic retrieveTopic;
  private final Topic terminatedTopic;
  private final MutableDirectBuffer mutableBuffer = new UnsafeBuffer(sendBuffer);
  private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
  private final DirectBuffer immutableBuffer = new UnsafeBuffer(new byte[0]);
  private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
  private final SequenceDecoder sequenceDecoder = new SequenceDecoder();
  private final RetransmissionDecoder retransmissionDecoder = new RetransmissionDecoder();


  protected RecoverableFlowReceiver(Builder builder) {
    super(builder);
    Objects.requireNonNull(messageConsumer);
    int offset = 0;
    frameEncoder.wrap(sendBuffer, offset).encodeFrameHeader();
    offset += frameEncoder.getHeaderLength();
    messageHeaderEncoder.wrap(mutableBuffer, offset);
    messageHeaderEncoder.blockLength(retransmitRequestEncoder.sbeBlockLength())
        .templateId(retransmitRequestEncoder.sbeTemplateId()).schemaId(retransmitRequestEncoder.sbeSchemaId())
        .version(retransmitRequestEncoder.sbeSchemaVersion());
    offset += messageHeaderEncoder.encodedLength();
    retransmitRequestEncoder.wrap(mutableBuffer, offset);
    frameEncoder.setMessageLength(offset + retransmitRequestEncoder.encodedLength());
    frameEncoder.encodeFrameTrailer();

    retrieveTopic = SessionEventTopics.getTopic(SERVICE_STORE_RETREIVE);
    terminatedTopic = SessionEventTopics.getTopic(sessionId, PEER_TERMINATED);
    finishedTopic = SessionEventTopics.getTopic(sessionId, SESSION_FINISHED);

    final Topic heartbeatTopic = SessionEventTopics.getTopic(sessionId, PEER_HEARTBEAT);
    heartbeatSubscription = reactor.subscribe(heartbeatTopic, heartbeatEvent);
    heartbeatSchedule = reactor.postAtInterval(heartbeatTopic, null, keepaliveInterval);
  }

  public void accept(ByteBuffer buffer) {
    immutableBuffer.wrap(buffer);
    int offset = buffer.position();
    messageHeaderDecoder.wrap(immutableBuffer, offset);
    boolean isApplicationMessage = true;
    offset += messageHeaderDecoder.encodedLength();

    if (messageHeaderDecoder.schemaId() == sequenceDecoder.sbeSchemaId()) {
 
      switch (messageHeaderDecoder.templateId()) {
      case SequenceDecoder.TEMPLATE_ID:
        sequenceDecoder.wrap(immutableBuffer, offset,
        sequenceDecoder.sbeBlockLength(), sequenceDecoder.sbeSchemaVersion());
        onSequence(sequenceDecoder);
        isApplicationMessage = false;
        break;
      case RetransmissionDecoder.TEMPLATE_ID:
        retransmissionDecoder.wrap(immutableBuffer, offset,
        retransmissionDecoder.sbeBlockLength(), sequenceDecoder.sbeSchemaVersion());
        onRetransmission(retransmissionDecoder, buffer);
        isApplicationMessage = false;
        break;
      case FinishedSendingDecoder.TEMPLATE_ID:
        finished(buffer);
        isApplicationMessage = false;
        break;
      case TerminateDecoder.TEMPLATE_ID:
        terminated(buffer);
        isApplicationMessage = false;
        break;
      default:
//          System.out
//              .println("RecoverableFlowReceiver: Protocol violation; unexpected session message "
//                  + decoder.getMessageType());
        reactor.post(terminatedTopic, buffer);
      }
    }
    if (isApplicationMessage && !isEndOfStream) {
      if (!isRetransmission.get()) {
        final long seqNo = nextSeqNoReceived.getAndIncrement();
        if (nextSeqNoAccepted.compareAndSet(seqNo, seqNo)) {
          nextSeqNoAccepted.incrementAndGet();
          messageConsumer.accept(buffer, session, seqNo);
        }
      } else {
        final long seqNo = nextRetransSeqNoReceived.getAndIncrement();
        if (seqNo <= lastRetransSeqNoToAccept) {
          messageConsumer.accept(buffer, session, seqNo);
        }
      }
    }
  }

  void onRetransmission(RetransmissionDecoder decoder, ByteBuffer buffer) {
    for (int i = 0; i < 16; i++) {
      retransSessionId[i] = (byte) decoder.sessionId(i);
    }
    long retransSeqNo = decoder.nextSeqNo();
    long timestamp = decoder.requestTimestamp();
    long count = decoder.count();

    if (timestamp == lastRequestTimestamp && Arrays.equals(uuidAsBytes, retransSessionId)) {
      nextRetransSeqNoReceived.set(retransSeqNo);
      lastRetransSeqNoToAccept = retransSeqNo + count;
      isRetransmission.set(true);
    } else {
      // System.err.println("Protocol violation; unsolicited retransmission");
      reactor.post(terminatedTopic, buffer);
    }
  }

  void onSequence(SequenceDecoder sequenceDecoder) {
    isHeartbeatDue.set(false);
    final long newNextSeqNo = sequenceDecoder.nextSeqNo();
    final long prevNextSeqNo = nextSeqNoReceived.getAndSet(newNextSeqNo);
    // todo: protocol violation if less than previous seq?
    final long accepted = nextSeqNoAccepted.get();
    isRetransmission.set(false);
    // System.out.format("Seq no: %d; prev nextSeqNo %d; accepted %d\n",
    // newNextSeqNo, prevNextSeqNo, accepted);
    if (newNextSeqNo > accepted) {
      if (newNextSeqNo > accepted) {
        // todo: make only one retrans request at a time - queue them up
        // or consolidate?
        notifyGap(prevNextSeqNo, (int) (newNextSeqNo - prevNextSeqNo));
      }
      // Continue to accept messages after the gap as if the missing
      // messages were received
      nextSeqNoAccepted.set(newNextSeqNo);
    }
  }

  private void finished(ByteBuffer buffer) {
    isEndOfStream = true;
    buffer.rewind();
    reactor.post(finishedTopic, buffer);
  }

  public long getNextSeqNo() {
    return nextSeqNoReceived.get();
  }

  @Override
  public boolean isHeartbeatDue() {
    return isHeartbeatDue.getAndSet(true);
  }

  void notifyGap(long fromSeqNo, int count) {
    for (int i = 0; i < 16; i++) {
      retransmitRequestEncoder.sessionId(i, uuidAsBytes[i]);
    }
    lastRequestTimestamp = System.nanoTime();
    retransmitRequestEncoder.timestamp(lastRequestTimestamp);
    retransmitRequestEncoder.fromSeqNo(fromSeqNo);
    retransmitRequestEncoder.count(count);
    // Post this to reactor for async message retrieval and retransmission
    reactor.post(retrieveTopic, sendBuffer);
  }

  private void terminated(ByteBuffer buffer) {
    isEndOfStream = true;
    buffer.rewind();
    reactor.post(terminatedTopic, buffer);
    heartbeatSchedule.cancel();
    heartbeatSubscription.unsubscribe();
  }
}
