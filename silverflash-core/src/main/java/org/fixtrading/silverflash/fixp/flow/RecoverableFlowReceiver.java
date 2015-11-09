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

import static org.fixtrading.silverflash.fixp.SessionEventTopics.FromSessionEventType.SESSION_FINISHED;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.ServiceEventType.SERVICE_STORE_RETREIVE;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.PEER_HEARTBEAT;
import static org.fixtrading.silverflash.fixp.SessionEventTopics.SessionEventType.PEER_TERMINATED;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.fixtrading.silverflash.Receiver;
import org.fixtrading.silverflash.Sequenced;
import org.fixtrading.silverflash.fixp.SessionEventTopics;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.Decoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.RetransmissionDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageDecoder.SequenceDecoder;
import org.fixtrading.silverflash.fixp.messages.MessageEncoder.RetransmissionRequestEncoder;
import org.fixtrading.silverflash.fixp.messages.MessageType;
import org.fixtrading.silverflash.fixp.messages.SbeMessageHeaderDecoder;
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
  private final MessageDecoder messageDecoder = new MessageDecoder();
  private final AtomicLong nextRetransSeqNoReceived = new AtomicLong(1);
  private final AtomicLong nextSeqNoAccepted = new AtomicLong(1);
  private final AtomicLong nextSeqNoReceived = new AtomicLong(1);
  private final ByteBuffer retransmissionRequestBuffer = ByteBuffer.allocateDirect(64)
      .order(ByteOrder.nativeOrder());
  private final RetransmissionRequestEncoder retransmissionRequestEncoder;
  private final byte[] retransSessionId = new byte[16];
  private final Topic retrieveTopic;
  private final Topic terminatedTopic;

  protected RecoverableFlowReceiver(Builder builder) {
    super(builder);
    retransmissionRequestEncoder = (RetransmissionRequestEncoder) messageEncoder
        .wrap(retransmissionRequestBuffer, 0, MessageType.RETRANSMIT_REQUEST);
    retransmissionRequestBuffer
        .limit(SbeMessageHeaderDecoder.getLength() + retransmissionRequestEncoder.getBlockLength());
    retrieveTopic = SessionEventTopics.getTopic(SERVICE_STORE_RETREIVE);
    terminatedTopic = SessionEventTopics.getTopic(sessionId, PEER_TERMINATED);
    finishedTopic = SessionEventTopics.getTopic(sessionId, SESSION_FINISHED);

    final Topic heartbeatTopic = SessionEventTopics.getTopic(sessionId, PEER_HEARTBEAT);
    heartbeatSubscription = reactor.subscribe(heartbeatTopic, heartbeatEvent);
    heartbeatSchedule = reactor.postAtInterval(heartbeatTopic, null, keepaliveInterval);
  }

  public void accept(ByteBuffer buffer) {
    Optional<Decoder> optDecoder = messageDecoder.wrap(buffer, buffer.position());
    // if not a known message type, assume it's an application
    // message
    boolean isApplicationMessage = true;
    if (optDecoder.isPresent()) {
      final Decoder decoder = optDecoder.get();
      switch (decoder.getMessageType()) {
      case SEQUENCE:
        accept((SequenceDecoder) decoder);
        isApplicationMessage = false;
        break;
      case RETRANSMISSION:
        accept((RetransmissionDecoder) decoder);
        isApplicationMessage = false;
        break;
      case FINISHED_SENDING:
        finished(buffer);
        isApplicationMessage = false;
        break;
      case TERMINATE:
        terminated(buffer);
        isApplicationMessage = false;
        break;
      default:
        System.err.println("Protocol violation");
      }
    }
    if (isApplicationMessage && !isEndOfStream) {
      if (!isRetransmission.get()) {
        final long seqNo = nextSeqNoReceived.getAndIncrement();
        if (nextSeqNoAccepted.compareAndSet(seqNo, seqNo)) {
          nextSeqNoAccepted.incrementAndGet();
          streamReceiver.accept(buffer, session, seqNo);
        }
      } else {
        final long seqNo = nextRetransSeqNoReceived.getAndIncrement();
        if (seqNo <= lastRetransSeqNoToAccept) {
          streamReceiver.accept(buffer, session, seqNo);
        }
      }
    }
  }

  void accept(RetransmissionDecoder decoder) {
    decoder.getSessionId(retransSessionId, 0);
    long retransSeqNo = decoder.getNextSeqNo();
    long timestamp = decoder.getRequestTimestamp();
    int count = decoder.getCount();

    if (timestamp == lastRequestTimestamp && Arrays.equals(uuidAsBytes, retransSessionId)) {
      nextRetransSeqNoReceived.set(retransSeqNo);
      lastRetransSeqNoToAccept = retransSeqNo + count;
      isRetransmission.set(true);
    } else {
      System.err.println("Protocol violation; unsolicited retransmission");
    }
  }

  void accept(SequenceDecoder sequenceDecoder) {
    isHeartbeatDue.set(false);
    final long newNextSeqNo = sequenceDecoder.getNextSeqNo();
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
    retransmissionRequestEncoder.setSessionId(uuidAsBytes);
    lastRequestTimestamp = System.nanoTime();
    retransmissionRequestEncoder.setTimestamp(lastRequestTimestamp);
    retransmissionRequestEncoder.setFromSeqNo(fromSeqNo);
    retransmissionRequestEncoder.setCount(count);
    // Post this to reactor for async message retrieval and retransmission
    reactor.post(retrieveTopic, retransmissionRequestBuffer);
  }

  private void terminated(ByteBuffer buffer) {
    isEndOfStream = true;
    buffer.rewind();
    reactor.post(terminatedTopic, buffer);
    heartbeatSchedule.cancel();
    heartbeatSubscription.unsubscribe();
  }
}
